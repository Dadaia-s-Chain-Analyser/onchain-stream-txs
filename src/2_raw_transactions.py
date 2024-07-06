import argparse
from random import random
import uuid
import hexbytes
import json
import logging
import time
import os
import redis
from datetime import datetime as dt

from requests.exceptions import HTTPError
from web3.exceptions import TransactionNotFound

from azure.identity import DefaultAzureCredential
from dadaia_tools.azure_key_vault_client import KeyVaultAPI
from confluent_kafka import Producer, Consumer
from confluent_kafka.error import KafkaError, KafkaException
from configparser import ConfigParser
from cassandra.cluster import Cluster

from dm_utilities.dm_utils import DataMasterUtils
from dm_utilities.dm_BNaaS_connector import BlockchainNodeAsAServiceConnector
from dm_utilities.dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler



###################################################################################################
###########################    CLASSE QUE PROCESSA TRANSAÇÕES BRUTAS    ###########################

class RawTransactionsProcessor(BlockchainNodeAsAServiceConnector):

  def __init__(self, network, logger, akv_client):
    self.network = network                    # Rede da blockchain em que transações serão capturadas
    self.logger = logger                      # Objeto de logging
    self.akv_client = akv_client              # Cliente do Azure Key Vault
    self.utils = DataMasterUtils()
    self.actual_api_key = None                # API KEY atual usada para conexão com o Web3
    self.web3 = None                          # Conexão com o Web3


  def set_web3_node_connection(self, api_key_name, vendor="infura"):
    api_key_secret = self.akv_client.get_secret(api_key_name)
    self.web3 = self.get_node_connection(self.network, api_key_secret, vendor)
    self.actual_api_key = api_key_name


  def consuming_topic(self, consumer):
    while True:
      msg = consumer.poll(timeout=0.1)
      if msg: yield msg.value().decode('utf-8')

  def capture_tx_data(self, tx_id):
    try:
      tx_data = self.web3.eth.get_transaction(tx_id)
    except TransactionNotFound:
      self.logger.error(f"Transaction not found: {tx_id}")
      return
    self.logger.info(f"API_request;{self.actual_api_key}")
    return tx_data

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


###################################################################################################
###########################    CLASSE QUE PROCESSA TRANSAÇÕES BRUTAS    ###########################

class APIKEYManager:

  def __init__(self, scylla_session, redis_client, logger):
    self.scylla_session = scylla_session      # Sessão do ScyllaDB
    self.scylla_table = 'api_keys_node_providers'
    self.redis_client = redis_client          # Cliente do Redis
    self.redis_semaphore_dict = "API_KEYS_semaphore"
    self.logger = logger                      # Objeto de logging
    self.blacklist = []                       # Lista de API KEYS que estão em quarentena
    self.allowed_date_formats = ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]

  def parse_datetime(self, data):
    for formato in self.allowed_date_formats:
      try: 
        date_formatted = dt.strptime(data, formato)
      except ValueError: continue
      else: return date_formatted

  def get_api_keys_stored_in_cassandra(self):
    try:
      rows = self.scylla_session.execute(f'SELECT name FROM {self.scylla_table}')
      return [row.name for row in rows]
    except Exception as e:
      self.logger.error(f"Error while reading from ScyllaDB: {e}")
      return []
    
  def populate_api_keys_in_cassandra(self, api_keys_received):
    stored_api_keys = self.get_api_keys_stored_in_cassandra()
    missing_stored_keys = list(set(api_keys_received) - set(stored_api_keys))
    query_base = f"INSERT INTO {self.scylla_table} "
    query_base += "(name, start, end, num_req_1d, last_req) VALUES"
    TEMPLATE_QUERY = lambda api_key, timestamp: f"{query_base} ('{api_key}', null, null, 0, '{timestamp}')"
    if len(missing_stored_keys) > 0:
      for api_key in missing_stored_keys:
        dt_now = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        query = TEMPLATE_QUERY(api_key, dt_now)
        self.scylla_session.execute(query)


  def get_data_from_scylla(self):
    rows = self.scylla_session.execute(f'SELECT * FROM {self.scylla_table}')
    all_api_keys_data = map(lambda x: (x.name, x.num_req_1d, x.last_req), rows)
    infura_api_keys_data = filter(lambda x: "infura" in x[0], all_api_keys_data)
    api_keys_sorted = sorted(infura_api_keys_data, key=lambda x: (x[1], x[2]))
    return [api_key[0] for api_key in api_keys_sorted]


  
  def set_key_as_busy(self, api_key_name):
    return {api_key_name: {"being_used": True, "last_request": str(dt.now().timestamp())}}

  def elect_new_api_key(self):
    api_keys_scylla = self.get_data_from_scylla()
    api_keys_used = self.redis_client.keys()
    for api_key in api_keys_scylla:
      if api_key not in api_keys_used:
        self.logger.info(f"API KEY ELECTED: {api_key}")
        return api_key
                     
  def free_api_keys(self, free_timeout=15):
    api_keys_data_cached = {api_key: self.redis_client.hgetall(api_key) for api_key in self.redis_client.keys()}
    api_keys_to_free = [api_key for api_key, value in api_keys_data_cached.items() if dt.now().timestamp() - float(value["last_update"]) > free_timeout]
    for api_key in api_keys_to_free:
      self.redis_client.delete(api_key)
      self.logger.info(f"API KEY FREED: {api_key}")


def parse_api_keys(api_keys_compressed):
  """
  Parse a compressed string of API KEYS into a list of API KEYS
  Ex. infura-1-3 -> [infura-1, infura-2, infura-3]
  """
  interval = [int(i) for i in api_keys_compressed.split("-")[-2:]]
  name_secret = "-".join(api_keys_compressed.split("-")[:-2])
  api_keys = [f"{name_secret}-{i}" for i in range(interval[0], interval[1] + 1)]
  return api_keys
  
if __name__ == '__main__':

  # Configurações de ambiente e argumentos
  APP_NAME="RAW_TXS_CRAWLER"
  NETWORK = os.environ["NETWORK"]
  KAFKA_BROKERS = {'bootstrap.servers': os.environ["KAFKA_CLUSTER"]}
  SCYLLA_HOST = os.environ["SCYLLA_HOST"]
  SCYLLA_PORT = os.environ["SCYLLA_PORT"]
  SCYLLA_KEYSPACE = os.environ["SCYLLA_KEYSPACE"]
  REDIS_HOST = os.environ["REDIS_HOST"]
  REDIS_PORT = os.environ["REDIS_PORT"]
  REDIS_PASS = os.environ["REDIS_PASS"]
  AKV_NAME = os.environ['KEY_VAULT_NODE_NAME']
  AKV_COMPACTED_SECRETS = os.environ['KEY_VAULT_NODE_SECRET']
  TX_THROUGHPUT_THRESHOLD = 100
  PROCESS_ID = f"job-{str(uuid.uuid4())[:4]}"


  REDIS_CLIENT = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS, decode_responses=True)
  SCYLLA_SESSION = Cluster([SCYLLA_HOST],port=SCYLLA_PORT).connect(SCYLLA_KEYSPACE,wait_for_all_pools=True)
  SCYLLA_SESSION.execute(f'USE {SCYLLA_KEYSPACE}')

  # Usa Azure Key vault para pegar as API KEYS guardadas lá como secrets.
  AKV_CLIENT = KeyVaultAPI(AKV_NAME, DefaultAzureCredential())

  parser = argparse.ArgumentParser(description=APP_NAME)
  parser.add_argument('config_producer', type=argparse.FileType('r'), help='Config Producers')
  parser.add_argument('config_consumer', type=argparse.FileType('r'), help='Config Consumers')
  args = parser.parse_args()

  # Parse de dados provenientes dos arquivos de configuração para variável config
  config = ConfigParser()
  config.read_file(args.config_producer)
  config.read_file(args.config_consumer)

  # Nome de tópicos provenientes do arquivo de configuração
  make_topic_name = lambda topic: f"{NETWORK}.{dict(config[topic])['topic']}"
  topic_out_logs = make_topic_name('topic.app.logs')
  topic_in_hash_id_txs = make_topic_name('topic.hash_txs')
  topic_out_raw_txs = make_topic_name('topic.raw_txs')

  # Criação de consumer para o tópico de hash_tx_ids e 1 producer para o tópico de raw_txs
  create_producer = lambda special_config: Producer(**KAFKA_BROKERS, **config['producer.general.config'], **config[special_config])
  PRODUCER_RAW_TX_DATA = create_producer('producer.raw_txs')
  PRODUCER_LOGS = create_producer('producer.logs.application')
  CONSUMER_HASH_TX_IDS = Consumer(**KAFKA_BROKERS, **config['consumer.hash_txs'])
  CONSUMER_HASH_TX_IDS.subscribe([topic_in_hash_id_txs])

  # Configurando Logging para console e Kafka
  LOGGER = logging.getLogger(f"{APP_NAME}_{PROCESS_ID}")
  LOGGER.setLevel(logging.INFO)
  kafka_handler = KafkaLoggingHandler(PRODUCER_LOGS, topic_out_logs)
  ConsoleLoggingHandler = ConsoleLoggingHandler()
  LOGGER.addHandler(ConsoleLoggingHandler)
  LOGGER.addHandler(kafka_handler)

  api_keys_received_as_parm = parse_api_keys(AKV_COMPACTED_SECRETS)
  api_key_arbitrator = APIKEYManager(SCYLLA_SESSION, REDIS_CLIENT, LOGGER)  # Instancia objeto para gerenciamento de API KEYS
  raw_tx_processor = RawTransactionsProcessor(NETWORK, LOGGER, AKV_CLIENT)        # Instancia objeto para processamento de transações brutas
  
  update_key = lambda key, timestamp: REDIS_CLIENT.hset(key, mapping={"process": PROCESS_ID, "last_update": timestamp})

  api_key_arbitrator.free_api_keys()
  api_key_arbitrator.populate_api_keys_in_cassandra(api_keys_received_as_parm)
  actual_api_key = api_key_arbitrator.elect_new_api_key()
  raw_tx_processor.set_web3_node_connection(actual_api_key)
  update_key(actual_api_key, dt.now().timestamp())

  counter = 0
  for msg in raw_tx_processor.consuming_topic(CONSUMER_HASH_TX_IDS):
    tx_data = raw_tx_processor.capture_tx_data(msg)
    if not tx_data: continue
    parsed_tx_data = raw_tx_processor.parse_tx_raw_data(tx_data)
    raw_tx_processor.produce_tx_raw_data(producer=PRODUCER_RAW_TX_DATA, topic=topic_out_raw_txs, data=parsed_tx_data)

    check_for_api_key_usage = REDIS_CLIENT.hget(actual_api_key, "process")
    if check_for_api_key_usage != PROCESS_ID:
      LOGGER.info(f"API KEY {actual_api_key} is being used by another process.")
      new_api_key = api_key_arbitrator.elect_new_api_key()
      if new_api_key: 
        actual_api_key = new_api_key
        raw_tx_processor.set_web3_node_connection(actual_api_key)

    update_key(actual_api_key, dt.now().timestamp())

    if counter % TX_THROUGHPUT_THRESHOLD == 0:
      LOGGER.info(f"API KEY {actual_api_key} reached throughput threshold.")
      new_api_key = api_key_arbitrator.elect_new_api_key()
      if new_api_key: 
        REDIS_CLIENT.delete(actual_api_key)
        actual_api_key = new_api_key
        raw_tx_processor.set_web3_node_connection(actual_api_key)
        update_key(actual_api_key, dt.now().timestamp())

    if counter % 10 == 0:
      usage = REDIS_CLIENT.hgetall(actual_api_key)
      print(usage)
      api_key_arbitrator.free_api_keys()
    
    counter += 1

  

  # #     try:

  # #       
  # #     except HTTPError as e:
  # #       print(e)
  # #       api_key_name = raw_tx_processor.choose_new_api_key(api_key_semaphore, api_key_name)
  # #       raw_tx_processor.blacklist.append(api_key_name)
  # #       api_key_secret = akv_client.get_secret(api_key_name) 
  # #       raw_tx_processor.set_web3_node_connection(api_key_secret)
  # #       continue
    
  # #     except Exception as e:
  # #       #print(tx_data)
  # #       logger.error(f"Error processing message: {e}")
  # #     counter += 1
  # #     #break
      
  # # finally:
  # #   consumer_txs_hash.close()
  # #   producer_txs_raw.flush()
  # #   cassandra_cluster.shutdown()
  # #   logger.removeHandler(kafka_handler)
    

