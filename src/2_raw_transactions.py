import argparse
from random import random
import hexbytes
import json
import logging
import os
from hashlib import sha256

from azure.identity import DefaultAzureCredential
from dadaia_tools.azure_key_vault_client import KeyVaultAPI
from confluent_kafka import Producer, Consumer
from confluent_kafka.error import KafkaError, KafkaException
from configparser import ConfigParser
from cassandra.cluster import Cluster


from dm_utilities.dm_utils import DataMasterUtils
from dm_utilities.dm_BNaaS_connector import BlockchainNodeAsAServiceConnector
from dm_utilities.dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler


class RawTransactionsProcessor(BlockchainNodeAsAServiceConnector):

  def __init__(self, network, logger, akv_secret_name):
    self.network = network
    self.akv_secret_name = akv_secret_name
    self.utils = DataMasterUtils()
    self.logger = logger
    self.api_key = None

  def set_config_web3_connection(self, api_key, vendor="infura"):
    self.api_key = api_key
    self.web3 = self.get_node_connection(self.network, api_key, vendor)

  def consuming_topic(self, consumer):
    while True:
      msg = consumer.poll(timeout=0.1)
      if msg: yield msg.value().decode('utf-8')

  def process_msg(self, tx_id, api_key_name):
    tx_data = self.web3.eth.get_transaction(tx_id)
    self.logger.info(f"API_request;{api_key_name}")
    tx_data = {k: bytes.hex(v) if type(v) == hexbytes.main.HexBytes else v for k, v in tx_data.items()}
    if tx_data.get("accessList"):
      tx_data["accessList"] = [dict(i) for i in tx_data["accessList"]]
      tx_data["accessList"] = [self.utils.convert_hex_to_hexbytes(tx_data) for tx_data in tx_data["accessList"]]
    return tx_data
        
  

  def produce_proc_msg(self, producer, topic):
    encoded_message = json.dumps(tx_data).encode('utf-8')
    producer.produce(topic, value=encoded_message)
    producer.flush()
    self.logger.info(f"mined_block_ingested;{tx_data['hash']};{tx_data['blockNumber']}")


  def get_api_keys_stored_in_cassandra(self, session):
    try:
      rows = session.execute('SELECT name FROM api_keys_node_providers')
      api_keys_list = [row.name for row in rows]
    except Exception as e:
      logger.error(f"Error getting less used api key: {e}")
    else: return api_keys_list


  def get_api_key_stored_less_used(self, session):
    try:
      rows = session.execute('SELECT * FROM api_keys_node_providers')
      api_keys_data = [(row.name, row.num_req_1d) for row in rows]
      api_key_less_used = min(api_keys_data, key=lambda x: x[1])[0]
    except Exception as e:
      logger.error(f"Error getting less used api key: {e}")
    else: return api_key_less_used


if __name__ == '__main__':

  network = os.environ["NETWORK"]
  key_vault_node_name = os.environ['KEY_VAULT_NODE_NAME']
  key_vault_node_secret = os.environ['KEY_VAULT_NODE_SECRET']
  parser = argparse.ArgumentParser(description=f'Stream transactions network')
  parser.add_argument('config_producer', type=argparse.FileType('r'), help='Config Producers')
  parser.add_argument('config_consumer', type=argparse.FileType('r'), help='Config Consumers')
  args = parser.parse_args()
  change_api_key_limit = 1000

  # Parse de dados provenientes dos arquivos de configuração para variável config
  config = ConfigParser()
  config.read_file(args.config_producer)
  config.read_file(args.config_consumer)

  # Configuração de sessão do ScyllaDB
  cassandra_cluster = Cluster(['scylladb'],port=9042)
  cassandra_session = cassandra_cluster.connect('operations',wait_for_all_pools=True)
  cassandra_session.execute('USE operations')
  

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
  key_vault_api = KeyVaultAPI(key_vault_node_name, credential)
  raw_tx_processor = RawTransactionsProcessor(network, logger, key_vault_node_secret)


  def parse_api_keys(api_keys_compressed):
    interval = [int(i) for i in api_keys_compressed.split("-")[-2:]]
    name_secret = "-".join(api_keys_compressed.split("-")[:-2])
    api_keys = [f"{name_secret}-{i}" for i in range(interval[0], interval[1] + 1)]
    return api_keys

  # Verifica se as API KEYS recebidas por parâmetro estão armazenadas no Cassandra.
  api_keys_received_as_parm = parse_api_keys(key_vault_node_secret)
  api_keys_stored_in_cassandra = raw_tx_processor.get_api_keys_stored_in_cassandra(session=cassandra_session)
  missing_stored_keys = list(set(api_keys_received_as_parm) - set(api_keys_stored_in_cassandra))

  
  # Caso uma API KEY esteja na lista de API KEYS não armazenadas no Cassandra, ela é usada para inicializar o web3.
  if len(missing_stored_keys) > 0:
    api_key_name = missing_stored_keys.pop(0)
    api_key_secret = key_vault_api.get_secret(api_key_name)
    raw_tx_processor.set_config_web3_connection(api_key_secret)

  counter = 0
  new_change_api_key_limit = change_api_key_limit + random.randint(-5, 5) # Limites de troca de API KEYs randomizados em intervalo para evitar conflitos.
  try:
    for msg in raw_tx_processor.consuming_topic(consumer_txs_hash):

      if counter % new_change_api_key_limit == 0 or not raw_tx_processor.web3:
        api_key_name = raw_tx_processor.get_api_key_stored_less_used(session=cassandra_session)  # Pega chave menos usada
        api_key_secret = key_vault_api.get_secret(api_key_name)                                  # Busca em Key Vault a API KEY pelo nome
        raw_tx_processor.set_config_web3_connection(api_key_secret)                              # Inicializa web3 com API KEY menos usada

      tx_data = raw_tx_processor.process_msg(msg, api_key_name)   # Processa a mensagem loggando consumo de API KEY
      raw_tx_processor.produce_proc_msg(producer=producer_txs_raw, topic=topic_out_raw_txs)

      counter += 1
  finally: 
    consumer_txs_hash.close()
    producer_txs_raw.flush()
    cassandra_cluster.shutdown()
    logger.removeHandler(kafka_handler)

