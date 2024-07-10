import argparse
import uuid
import json
import logging
import os
from redis import Redis

from requests.exceptions import HTTPError
from web3.exceptions import TransactionNotFound

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from confluent_kafka import Producer, Consumer
from configparser import ConfigParser
from cassandra.cluster import Cluster, Session

from utils.dm_utils import DataMasterUtils
from utils.blockchain_node_connector import BlockchainNodeConnector
from utils.dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler
from utils.api_keys_manager import APIKeysManager


class RawTransactionsProcessor(BlockchainNodeConnector):

  def __init__(self, network: str, logger, akv_client):
    super().__init__(logger, akv_client, network)
    self.utils = DataMasterUtils()
    self.actual_api_key = None
    self.web3 = None                          


  def set_web3_node_connection(self, api_key_name):
    self.web3 = super().get_node_connection(api_key_name, 'infura')
    self.actual_api_key = api_key_name


  def consuming_topic(self, consumer):
    while True:
      msg = consumer.poll(timeout=0.1)
      if msg: yield msg.key().decode('utf-8')


  def parse_to_transaction_schema(self, tx_data):
    access_list = tx_data.get("accessList")
    if access_list:
      access_list = [dict(access_item) for access_item in tx_data.get("accessList")]
      access_list = [{"address": item["address"], "storageKeys": [bytes.hex(i) for i in item["storageKeys"]]} for item in access_list]
    else:
      access_list = []
    return {
      "blockHash": bytes.hex(tx_data["blockHash"]),
      "blockNumber": tx_data["blockNumber"],
      "hash": bytes.hex(tx_data["hash"]),
      "transactionIndex": tx_data["transactionIndex"],
      "from": tx_data["from"],
      "to": tx_data["to"],
      "value": tx_data["value"],
      "input": bytes.hex(tx_data["input"]),
      "gas": tx_data["gas"],
      "gasPrice": tx_data["gasPrice"],
      "maxFeePerGas": tx_data.get("maxFeePerGas"),
      "maxPriorityFeePerGas": tx_data.get("maxPriorityFeePerGas"),
      "nonce": tx_data["nonce"],
      "v": tx_data["v"],
      "r": bytes.hex(tx_data["r"]),
      "s": bytes.hex(tx_data["s"]),
      "type": tx_data["type"],
      "accessList": access_list
    }

  def capture_tx_data(self, tx_id):
    try: 
      tx_data = self.web3.eth.get_transaction(tx_id)
      self.logger.info(f"API_request;{self.actual_api_key}")
      return tx_data
    except TransactionNotFound:
      self.logger.error(f"Transaction not found: {tx_id}")
  

  def produce_tx_raw_data(self, producer, topic, data):
    encoded_message = json.dumps(data).encode('utf-8')
    producer.produce(topic, value=encoded_message)
    producer.flush()
    self.logger.info(f"mined_block_ingested;{data['hash']};{data['blockNumber']}")


  def check_type_transaction(self, tx_data):
    if tx_data['to'] == None: return 2
    elif tx_data['input'] == '': return 1
    else: return 3

  

if __name__ == '__main__':

  APP_NAME="RAW_TXS_CRAWLER"
  NETWORK = os.getenv("NETWORK")
  KAFKA_BROKERS = {'bootstrap.servers': os.getenv("KAFKA_BROKERS")}
  TOPIC_LOGS = os.getenv("TOPIC_LOGS")
  TOPIC_TX_HASH_IDS = os.getenv("TOPIC_TX_HASH_IDS")
  TOPIC_TX_CONTRACT_DEPLOY = os.getenv("TOPIC_TX_CONTRACT_DEPLOY")
  TOPIC_TX_CONTRACT_CALL = os.getenv("TOPIC_TX_CONTRACT_CALL")
  TOPIC_TX_TOKEN_TRANSFER = os.getenv("TOPIC_TX_TOKEN_TRANSFER")

  SCYLLA_HOST = os.getenv("SCYLLA_HOST")
  SCYLLA_PORT = os.getenv("SCYLLA_PORT")
  SCYLLA_KEYSPACE = os.getenv("SCYLLA_KEYSPACE")
  SCYLLA_TABLE = os.getenv("SCYLLA_TABLE")
  REDIS_HOST = os.getenv("REDIS_HOST")
  REDIS_PORT = os.getenv("REDIS_PORT")
  REDIS_PASS = os.getenv("REDIS_PASS")
  AKV_NAME = os.getenv("AKV_NAME")
  AKV_COMPACTED_SECRETS = os.getenv('AKV_SECRET_NAMES')
  TX_THROUGHPUT_THRESHOLD = 100
  PROCESS_ID = f"job-{str(uuid.uuid4())[:4]}"
  COUNTER = 0

  REDIS_CLIENT: Redis = Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS, decode_responses=True)
  SCYLLA_SESSION: Session = Cluster([SCYLLA_HOST],port=SCYLLA_PORT).connect(SCYLLA_KEYSPACE,wait_for_all_pools=True)
  SCYLLA_SESSION.execute(f'USE {SCYLLA_KEYSPACE}')

  AKV_URL = f'https://{AKV_NAME}.vault.azure.net/'
  AKV_CLIENT = SecretClient(vault_url=AKV_URL, credential=DefaultAzureCredential())

  parser = argparse.ArgumentParser(description=APP_NAME)
  parser.add_argument('config_producer', type=argparse.FileType('r'), help='Config Producers')
  parser.add_argument('config_consumer', type=argparse.FileType('r'), help='Config Consumers')
  args = parser.parse_args()

  # Parse de dados provenientes dos arquivos de configuração para variável config
  config = ConfigParser()
  config.read_file(args.config_producer)
  config.read_file(args.config_consumer)

  # Criação de consumer para o tópico de hash_tx_ids e 1 producer para o tópico de raw_txs
  create_producer = lambda special_config: Producer(**KAFKA_BROKERS, **config['producer.general.config'], **config[special_config])
  PRODUCER_RAW_TX_DATA = create_producer('producer.raw_txs')
  PRODUCER_LOGS = create_producer('producer.logs.application')
  CONSUMER_HASH_TX_IDS = Consumer(**KAFKA_BROKERS, **config['consumer.hash_txs'])
  CONSUMER_HASH_TX_IDS.subscribe([TOPIC_TX_HASH_IDS])

  # Configurando Logging para console e Kafka
  LOGGER = logging.getLogger(f"{APP_NAME}_{PROCESS_ID}")
  LOGGER.setLevel(logging.INFO)
  kafka_handler = KafkaLoggingHandler(PRODUCER_LOGS, TOPIC_LOGS)
  ConsoleLoggingHandler = ConsoleLoggingHandler()
  LOGGER.addHandler(ConsoleLoggingHandler)
  LOGGER.addHandler(kafka_handler)

  api_key_arbitrator = APIKeysManager(LOGGER, PROCESS_ID, SCYLLA_SESSION, SCYLLA_TABLE, REDIS_CLIENT)
  raw_tx_processor = RawTransactionsProcessor(NETWORK, LOGGER, AKV_CLIENT)
  api_keys_received = api_key_arbitrator.decompress_api_key_names(AKV_COMPACTED_SECRETS)
  
  api_key_arbitrator.free_api_keys()
  api_key_arbitrator.populate_scylla_with_missing_api_keys(api_keys_received)

  actual_api_key = api_key_arbitrator.elect_new_api_key()
  raw_tx_processor.set_web3_node_connection(actual_api_key)
  api_key_arbitrator.check_api_key_request(actual_api_key)
  
  DICT_TYPE_TRANSACTIONS = {
    1: TOPIC_TX_TOKEN_TRANSFER,
    2: TOPIC_TX_CONTRACT_DEPLOY,
    3: TOPIC_TX_CONTRACT_CALL
  }

  for msg in raw_tx_processor.consuming_topic(CONSUMER_HASH_TX_IDS):
    raw_transaction_data = raw_tx_processor.capture_tx_data(msg)
    cleaned_transaction_data = raw_tx_processor.parse_to_transaction_schema(raw_transaction_data)
    # if not tx_data: continue
    api_key_arbitrator.check_api_key_request(actual_api_key)
    type_transaction = raw_tx_processor.check_type_transaction(cleaned_transaction_data)
    raw_tx_processor.produce_tx_raw_data(
      producer=PRODUCER_RAW_TX_DATA,
      topic=DICT_TYPE_TRANSACTIONS[type_transaction],
      data=cleaned_transaction_data)

    if REDIS_CLIENT.hget(actual_api_key, "process") != PROCESS_ID:
      LOGGER.info(f"API KEY {actual_api_key} is being used by another process.")
      new_api_key = api_key_arbitrator.elect_new_api_key()
      if new_api_key: 
        actual_api_key = new_api_key
        raw_tx_processor.set_web3_node_connection(actual_api_key)

    if COUNTER % TX_THROUGHPUT_THRESHOLD == 0:
      LOGGER.info(f"API KEY {actual_api_key} reached throughput threshold.")
      new_api_key = api_key_arbitrator.elect_new_api_key()
      if new_api_key: 
        REDIS_CLIENT.delete(actual_api_key)
        actual_api_key = new_api_key
        raw_tx_processor.set_web3_node_connection(actual_api_key)
        api_key_arbitrator.check_api_key_request(actual_api_key)

    if COUNTER % 10 == 0:
      api_key_arbitrator.free_api_keys()
    
    COUNTER += 1