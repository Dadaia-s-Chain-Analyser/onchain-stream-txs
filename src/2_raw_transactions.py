import argparse
from random import random
import uuid
import hexbytes
import json
import logging
import time
import os
from hashlib import sha256
import random
from datetime import datetime as dt

from requests.exceptions import HTTPError

from azure.identity import DefaultAzureCredential
from dadaia_tools.azure_key_vault_client import KeyVaultAPI
from confluent_kafka import Producer, Consumer
from confluent_kafka.error import KafkaError, KafkaException
from configparser import ConfigParser
from cassandra.cluster import Cluster

from dm_utilities.dm_utils import DataMasterUtils
from dm_utilities.dm_BNaaS_connector import BlockchainNodeAsAServiceConnector
from dm_utilities.dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler
from dm_utilities.redis_client import RedisClient



class RawTransactionsProcessor(BlockchainNodeAsAServiceConnector):

  def __init__(self, network, scylla_session, redis_client, akv_client, logger, akv_secret_name):
    self.pid = uuid.uuid4().hex[:8]           # Identificador único do processo
    self.network = network                    # Rede da blockchain em que transações serão capturadas
    self.scylla_session = scylla_session      # Sessão do ScyllaDB
    self.redis_client = redis_client          # Cliente do Redis
    self.akv_client = akv_client              # Cliente do Azure Key Vault
    self.logger = logger                      # Objeto de logging
    self.akv_secret_name = akv_secret_name    # Azure Key Vault secret name

    self.utils = DataMasterUtils()
    self.blacklist = []                       # Lista de API KEYS que estão em quarentena
    self.actual_api_key = None                # API KEY atual usada para conexão com o Web3
    self.web3 = None                          # Conexão com o Web3
    self.allowed_date_formats = ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]


  def set_config_web3_connection(self, api_key_secret, vendor="infura"):
    self.web3 = self.get_node_connection(self.network, api_key_secret, vendor)
    self.actual_api_key = api_key_secret


  def consuming_topic(self, consumer):
    while True:
      msg = consumer.poll(timeout=0.1)
      if msg: yield msg.value().decode('utf-8')


  def capture_tx_data(self, tx_id, api_key_name):
    tx_data = self.web3.eth.get_transaction(tx_id)
    self.logger.info(f"API_request;{api_key_name}")
    return tx_data



  def parse_datetime(self, data):
    for formato in self.allowed_date_formats:
      try: 
        date_formatted = dt.strptime(data, formato)
      except ValueError: continue
      else: return date_formatted


  def parse_tx_raw_data(self, tx_data):
    tx_data = {k: bytes.hex(v) if type(v) == hexbytes.main.HexBytes else v for k, v in tx_data.items()}
    if tx_data.get("accessList"):
      tx_data["accessList"] = [dict(i) for i in tx_data["accessList"]]
      tx_data["accessList"] = [self.utils.convert_hex_to_hexbytes(tx_data) for tx_data in tx_data["accessList"]]
    if tx_data.get("blobVersionedHashes"):
      tx_data["blobVersionedHashes"] = [self.utils.convert_hex_to_hexbytes(tx_data) for tx_data in tx_data["blobVersionedHashes"]]
    return tx_data
        
  
  def produce_tx_raw_data(self, producer, topic, data):
    encoded_message = json.dumps(data).encode('utf-8')
    producer.produce(topic, value=encoded_message)
    producer.flush()
    self.logger.info(f"mined_block_ingested;{data['hash']};{data['blockNumber']}")


  def get_api_keys_stored_in_cassandra(self):
    try:
      rows = self.scylla_session.execute('SELECT name FROM api_keys_node_providers')
      return [row.name for row in rows]
    except Exception as e:
      logger.error(f"Error while reading from ScyllaDB: {e}")
      return []
    

  def __get_api_key_less_used_today(self, busy_keys):
    rows = self.scylla_session.execute('SELECT * FROM api_keys_node_providers')
    all_api_keys_data = list(map(lambda x: (x.name, x.num_req_1d, x.last_req), rows))
    infura_api_keys_data = list(filter(lambda x: "infura" in x[0], all_api_keys_data))
    infura_api_keys_data_f = list(map(lambda x: (x[0], x[1], self.parse_datetime(x[2])), infura_api_keys_data))
    api_keys_not_used_today = list(filter(lambda x: x[2].date() != dt.now().date(), infura_api_keys_data_f))
    if len(api_keys_not_used_today) > 0:
      less_used_api_key = api_keys_not_used_today[0][0]
    else:
      free_api_keys_data = list(filter(lambda x: x[0] not in busy_keys, infura_api_keys_data_f))
      less_used_api_key = min(free_api_keys_data, key=lambda x: x[1])[0]
    return less_used_api_key


  def __get_busy_api_keys(self, cache):
    busy_keys_data = list(filter(lambda x: x[1]["being_used"] == True, cache.items()))
    busy_keys_name = list(map(lambda x: x[0], busy_keys_data))
    return busy_keys_name


  def choose_new_api_key(self, api_key_semaphore, old_api_key_name):
    start_time = time.time()
    cached_api_key_semaphore = redis_client.get_key_obj(api_key_semaphore)
    busy_keys_name = self.__get_busy_api_keys(cached_api_key_semaphore)
    less_used_api_key = self.__get_api_key_less_used_today(busy_keys_name)

    cached_api_key_semaphore[less_used_api_key]["being_used"] = True
    redis_client.insert_key_obj(api_key_semaphore, cached_api_key_semaphore)
    elapsed_time = time.time() - start_time
    print(f"Time to choose new API KEY and cache it: {elapsed_time} seconds")
    cached_api_key_semaphore = redis_client.get_key_obj(api_key_semaphore)
    if old_api_key_name:
      cached_api_key_semaphore[old_api_key_name]["being_used"] = False
    cached_api_key_semaphore[less_used_api_key]["last_request"] = str(dt.now().timestamp())
    self.redis_client.insert_key_obj(api_key_semaphore, cached_api_key_semaphore)
    api_key_secret = self.akv_client.get_secret(less_used_api_key)
    self.set_config_web3_connection(api_key_secret)
    return less_used_api_key


  def free_api_keys(self, redis_client, api_key_semaphore):
    cached_api_key_semaphore = redis_client.get_key_obj(api_key_semaphore)
    timestamp_now = dt.now().timestamp()
    for k, v in cached_api_key_semaphore.items():
      if v["last_request"] and (timestamp_now - int(float(v["last_request"]))) > 120 or v["last_request"] == None:
        if v["being_used"]: print(f"Freeing {k}")
        cached_api_key_semaphore[k]["being_used"] = False
    redis_client.insert_key_obj(api_key_semaphore, cached_api_key_semaphore)
    


if __name__ == '__main__':

  # Configurações de ambiente e argumentos
  network = os.environ["NETWORK"]
  change_api_key_limit = 200
  REDIS_SERVER = dict(host="redis", port=6379)
  key_vault_node_name = os.environ['KEY_VAULT_NODE_NAME']
  key_vault_node_secret = os.environ['KEY_VAULT_NODE_SECRET']
  parser = argparse.ArgumentParser(description=f'Stream transactions network')
  parser.add_argument('config_producer', type=argparse.FileType('r'), help='Config Producers')
  parser.add_argument('config_consumer', type=argparse.FileType('r'), help='Config Consumers')
  parser.add_argument('general_config', type=argparse.FileType('r'), help='General Config')
  args = parser.parse_args()


  # Parse de dados provenientes dos arquivos de configuração para variável config
  config = ConfigParser()
  config.read_file(args.config_producer)
  config.read_file(args.config_consumer)
  config.read_file(args.general_config)

  # Configuração de sessão do ScyllaDB
  cassandra_host = dict(config['scylladb'])['host']
  cassandra_port = int(dict(config['scylladb'])['port'])
  cassandra_cluster = Cluster([cassandra_host],port=cassandra_port)
  scylla_session = cassandra_cluster.connect('operations',wait_for_all_pools=True)
  scylla_session.execute('USE operations')

  redis_client = RedisClient(**REDIS_SERVER)
  api_key_semaphore = "api_key_semaphore"
  
  # Nome de tópicos provenientes do arquivo de configuração
  make_topic_name = lambda topic: f"{network}.{dict(config[topic])['topic']}"
  topic_out_logs = make_topic_name('topic.app.logs')
  topic_in_hash_id_txs = make_topic_name('topic.hash_txs')
  topic_out_raw_txs = make_topic_name('topic.raw_txs')

  # Criação de 1 consumer para o tópico de hash_txs e 1 producer para o tópico de raw_txs
  create_producer = lambda special_config: Producer(**config['producer.general.config'], **config[special_config])
  producer_txs_raw = create_producer('producer.raw_txs')
  producer_logs = create_producer('producer.logs.block_clock')
  consumer_txs_hash = Consumer(**config['consumer.general.config'], **config['consumer.hash_txs'])
  consumer_txs_hash.subscribe([topic_in_hash_id_txs])

  # Configurando Logging para console e Kafka
  logger = logging.getLogger("raw-transactions")
  logger.setLevel(logging.INFO)
  kafka_handler = KafkaLoggingHandler(producer_logs, topic_out_logs)
  ConsoleLoggingHandler = ConsoleLoggingHandler()
  logger.addHandler(ConsoleLoggingHandler)
  logger.addHandler(kafka_handler)        

  # Usa Azure Key vault para pegar as API KEYS guardadas lá como secrets.
  credential = DefaultAzureCredential()
  akv_client = KeyVaultAPI(key_vault_node_name, credential)

  raw_tx_processor = RawTransactionsProcessor(network, scylla_session, redis_client, akv_client, logger, key_vault_node_secret)

  # Função que recebe uma string de API KEYS comprimida (ex. infura-1-3) e retorna uma lista de API KEYS (ex. [infura-1, infura-2, infura-3])
  def parse_api_keys(api_keys_compressed):
    interval = [int(i) for i in api_keys_compressed.split("-")[-2:]]
    name_secret = "-".join(api_keys_compressed.split("-")[:-2])
    api_keys = [f"{name_secret}-{i}" for i in range(interval[0], interval[1] + 1)]
    return api_keys
  
  api_keys_received_as_parm = parse_api_keys(key_vault_node_secret)

  # COMPUTA as API KEYS que estão faltando no Cassandra com base nas recebidas como parâmetro
  api_keys_stored_in_cassandra = raw_tx_processor.get_api_keys_stored_in_cassandra()
  if len(api_keys_stored_in_cassandra) == 0:
    for i in api_keys_received_as_parm:
      dt_now = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")
      raw_tx_processor.scylla_session.execute(f"INSERT INTO api_keys_node_providers (name, start, end, num_req_1d, last_req) VALUES ('{i}', null, null, 0, '{dt_now}')")
    api_keys_stored_in_cassandra = raw_tx_processor.get_api_keys_stored_in_cassandra()

  missing_stored_keys = list(set(api_keys_received_as_parm) - set(api_keys_stored_in_cassandra))

  # Caso API KEYS estejam faltando no Cassandra, pega a primeira e seta a conexão do web3
  if len(missing_stored_keys) > 0:
    api_key_name = missing_stored_keys.pop(0)
    api_key_secret = akv_client.get_secret(api_key_name)
    raw_tx_processor.set_config_web3_connection(api_key_secret)
  else:
    api_key_name = None


  # PREENCHE O CACHE COM AS API KEYS RECEBIDAS CASO NÃO ESTEJAM LÁ
  cached_api_keys = redis_client.get_key_obj(api_key_semaphore, default={})
  missing_cached_keys = list(set(api_keys_received_as_parm) - set(cached_api_keys.keys()))
  for i in missing_cached_keys:
    cached_api_keys[i] = {"being_used": False, "last_request": None}
  redis_client.insert_key_obj(api_key_semaphore, cached_api_keys)


  try:
    counter = 0
    for msg in raw_tx_processor.consuming_topic(consumer_txs_hash):       # LOOP CONSUMINDO MENSAGENS DO KAFKA
      if counter % change_api_key_limit == 0 or not raw_tx_processor.web3:
        raw_tx_processor.free_api_keys(redis_client, api_key_semaphore)
        new_api_key_name = raw_tx_processor.choose_new_api_key(api_key_semaphore, api_key_name)
        api_key_name = new_api_key_name

      try:
        tx_data = raw_tx_processor.capture_tx_data(msg, api_key_name)   # Processa a mensagem loggando consumo de API KEY
        parsed_tx_data = raw_tx_processor.parse_tx_raw_data(tx_data)    # Processa a mensagem loggando consumo de API KEY
        raw_tx_processor.produce_tx_raw_data(producer=producer_txs_raw, topic=topic_out_raw_txs, data=parsed_tx_data)
      except HTTPError as e:
        print(e)
        api_key_name = raw_tx_processor.choose_new_api_key(api_key_semaphore, api_key_name)
        raw_tx_processor.blacklist.append(api_key_name)
        api_key_secret = akv_client.get_secret(api_key_name) 
        raw_tx_processor.set_config_web3_connection(api_key_secret)
        continue
    
      except Exception as e:
        #print(tx_data)
        logger.error(f"Error processing message: {e}")
      counter += 1
      #break
      
  finally:
    consumer_txs_hash.close()
    producer_txs_raw.flush()
    cassandra_cluster.shutdown()
    logger.removeHandler(kafka_handler)
    

