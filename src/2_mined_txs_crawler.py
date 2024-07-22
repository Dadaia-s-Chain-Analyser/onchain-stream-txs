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

from confluent_kafka import SerializingProducer, Producer, Consumer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from configparser import ConfigParser
from cassandra.cluster import Cluster, Session

from utils.dm_utils import DataMasterUtils
from utils.blockchain_node_connector import BlockchainNodeConnector
from utils.dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler
from utils.api_keys_manager import APIKeysManager
from utils.kafka_handlers import SuccessHandler, ErrorHandler


class RawTransactionsProcessor(BlockchainNodeConnector):

  def __init__(self, network: str, logger, akv_client, schema_registry):
    super().__init__(logger, akv_client, network)
    self.schema_registry_url = schema_registry
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
      "to": tx_data["to"] if tx_data["to"] else "",
      "value": str(tx_data["value"]),
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

  def __get_transaction_avro_schema(self):
    with open('schemas/transactions_schema_avro.json') as f:
      return json.load(f)
    

  def create_serializable_producer(self, producer_configs) -> SerializingProducer:
    schema_registry_conf = {'url': self.schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_schema = json.dumps(self.__get_transaction_avro_schema())
    avro_serializer = AvroSerializer(
      schema_registry_client,
      avro_schema,
      lambda msg, ctx: dict(msg)
    )
    producer_configs['key.serializer'] = StringSerializer('utf_8')
    producer_configs['value.serializer'] = avro_serializer
    return SerializingProducer(producer_configs)


  def capture_tx_data(self, tx_id):
    try:
      tx_data = self.web3.eth.get_transaction(tx_id)
      self.logger.info(f"API_request;{self.actual_api_key}")
      return tx_data
    except TransactionNotFound:
      self.logger.error(f"Transaction not found: {tx_id}")

  def check_type_transaction(self, tx_data):
    if tx_data['to'] == None: return 2
    elif tx_data['input'] == '': return 1
    else: return 3

  def message_handler(self, err, msg):
    if err is not None: ErrorHandler(self.logger)(err)
    else: SuccessHandler(self.logger)(msg)

if __name__ == '__main__':

  APP_NAME="RAW_TXS_CRAWLER"
  NETWORK = os.getenv("NETWORK")
  KAFKA_BROKERS = {"bootstrap.servers": os.getenv("KAFKA_BROKERS")}
  TOPIC_LOGS = os.getenv("TOPIC_LOGS")
  TOPIC_TX_HASH_IDS = os.getenv("TOPIC_TX_HASH_IDS")
  GROUP_ID = os.getenv("GROUP_ID")
  TOPIC_TX_CONTRACT_DEPLOY = os.getenv("TOPIC_TX_CONTRACT_DEPLOY")
  TOPIC_TX_CONTRACT_CALL = os.getenv("TOPIC_TX_CONTRACT_CALL")
  TOPIC_TX_TOKEN_TRANSFER = os.getenv("TOPIC_TX_TOKEN_TRANSFER")
  SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")

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
  PROC_ID = f"job-{str(uuid.uuid4())[:8]}"
  
  LOGGER = logging.getLogger(f"{APP_NAME}_{PROC_ID}")
  LOGGER.setLevel(logging.INFO)
  LOGGER.addHandler(ConsoleLoggingHandler())


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

  general_conf_producer = {**KAFKA_BROKERS, "client.id": PROC_ID, **config['producer.general.config']}
  PRODUCER_LOGS = Producer(**general_conf_producer, **config['producer.config.p1'])

  general_conf_consumer = {**KAFKA_BROKERS, "group.id": GROUP_ID, **config['consumer.general.config']}
  CONSUMER_HASH_TX_IDS = Consumer(**general_conf_consumer, **{"client.id": PROC_ID})
  CONSUMER_HASH_TX_IDS.subscribe([TOPIC_TX_HASH_IDS])

  kafka_handler = KafkaLoggingHandler(PRODUCER_LOGS, TOPIC_LOGS)
  LOGGER.addHandler(kafka_handler)

  API_KEY_ARBITRATOR = APIKeysManager(LOGGER, PROC_ID, SCYLLA_SESSION, SCYLLA_TABLE, REDIS_CLIENT)
  TXS_PROCESSOR = RawTransactionsProcessor(NETWORK, LOGGER, AKV_CLIENT, SCHEMA_REGISTRY_URL)
  api_keys_received = API_KEY_ARBITRATOR.decompress_api_key_names(AKV_COMPACTED_SECRETS)
  
  PRODUCER_RAW_TX_DATA = TXS_PROCESSOR.create_serializable_producer(
    {**general_conf_producer, **config['producer.config.p1']}
  )

  API_KEY_ARBITRATOR.free_api_keys()
  API_KEY_ARBITRATOR.populate_scylla_with_missing_api_keys(api_keys_received)

  actual_api_key = API_KEY_ARBITRATOR.elect_new_api_key()
  TXS_PROCESSOR.set_web3_node_connection(actual_api_key)
  API_KEY_ARBITRATOR.check_api_key_request(actual_api_key)
  
  DICT_TYPE_TRANSACTIONS = {
    1: TOPIC_TX_TOKEN_TRANSFER,
    2: TOPIC_TX_CONTRACT_DEPLOY,
    3: TOPIC_TX_CONTRACT_CALL
  }

  counter = 0
  for msg in TXS_PROCESSOR.consuming_topic(CONSUMER_HASH_TX_IDS):
    raw_transaction_data = TXS_PROCESSOR.capture_tx_data(msg)
    API_KEY_ARBITRATOR.check_api_key_request(actual_api_key)
    if not raw_transaction_data: continue
    cleaned_transaction_data = TXS_PROCESSOR.parse_to_transaction_schema(raw_transaction_data)
    
    msg_key = str(cleaned_transaction_data['hash']) 
    type_transaction = TXS_PROCESSOR.check_type_transaction(cleaned_transaction_data)
    topic = DICT_TYPE_TRANSACTIONS[type_transaction]
    PRODUCER_RAW_TX_DATA.produce(topic, key=msg_key, value=cleaned_transaction_data, on_delivery=TXS_PROCESSOR.message_handler)
    PRODUCER_RAW_TX_DATA.poll(1)

    if REDIS_CLIENT.hget(actual_api_key, "process") != PROC_ID:
      LOGGER.info(f"API KEY {actual_api_key} is being used by another process.")
      new_api_key = API_KEY_ARBITRATOR.elect_new_api_key()
      if new_api_key:
        actual_api_key = new_api_key
        TXS_PROCESSOR.set_web3_node_connection(actual_api_key)

    if counter % TX_THROUGHPUT_THRESHOLD == 0:
      LOGGER.info(f"API KEY {actual_api_key} reached throughput threshold.")
      new_api_key = API_KEY_ARBITRATOR.elect_new_api_key()
      if new_api_key: 
        REDIS_CLIENT.delete(actual_api_key)
        actual_api_key = new_api_key
        TXS_PROCESSOR.set_web3_node_connection(actual_api_key)
        API_KEY_ARBITRATOR.check_api_key_request(actual_api_key)

    if counter % 10 == 0:
      API_KEY_ARBITRATOR.free_api_keys()
    
    counter += 1