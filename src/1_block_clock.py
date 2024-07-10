import argparse
import hexbytes
import json
import logging
import os
import time
import datetime as dt

from configparser import ConfigParser
from requests import HTTPError
from confluent_kafka import SerializingProducer, Producer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from utils.dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler
from utils.blockchain_node_connector import BlockchainNodeConnector
from utils.kafka_handlers import SuccessHandler, ErrorHandler


class MinedBlocksProcessor(BlockchainNodeConnector):

  def __init__(self, network, logger, akv_client, api_key_name, schema_registry_url):
    super().__init__(logger, akv_client, network)
    self.schema_registry_url = schema_registry_url
    self.web3 = super().get_node_connection(api_key_name, 'alchemy')
    self.api_key_name = api_key_name


  def __get_latest_block(self):
    try:
      block_info = self.web3.eth.get_block('latest')
      self.logger.info(f"API_request;{self.api_key_name}")
      return block_info
    except HTTPError as e:
      self.logger.error(f"API_request;{self.api_key_name};Error:{str(e)}")
      return
    
  def __get_block_clock_avro_schema(self):
    with open('schemas/block_metadata_avro.json') as f:
      return json.load(f) 
  

  def create_serializable_producer(self, producer_configs) -> SerializingProducer:
    schema_registry_conf = {'url': self.schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_schema = json.dumps(self.__get_block_clock_avro_schema())
    avro_serializer = AvroSerializer(
      schema_registry_client,
      avro_schema,
      lambda msg, ctx: dict(msg)
    )
    producer_configs['key.serializer'] = StringSerializer('utf_8')
    producer_configs['value.serializer'] = avro_serializer
    return SerializingProducer(producer_configs)


  def parse_to_block_clock_schema(self, block_raw_data):
    return {
      "number": block_raw_data['number'],
      "timestamp": block_raw_data['timestamp'],
      "hash": bytes.hex(block_raw_data['hash']),
      "parentHash": bytes.hex(block_raw_data['parentHash']),
      "difficulty": block_raw_data['difficulty'],
      "totalDifficulty": str(block_raw_data['totalDifficulty']),
      "nonce": bytes.hex(block_raw_data['nonce']),
      "size": block_raw_data['size'],
      "miner": block_raw_data['miner'],
      "baseFeePerGas": block_raw_data['baseFeePerGas'],
      "gasLimit": block_raw_data['gasLimit'],
      "gasUsed": block_raw_data['gasUsed'],
      "logsBloom": bytes.hex(block_raw_data['logsBloom']),
      "extraData": bytes.hex(block_raw_data['extraData']),
      "transactionsRoot": bytes.hex(block_raw_data['transactionsRoot']),
      "stateRoot": bytes.hex(block_raw_data['stateRoot']),
      "transactions": [bytes.hex(i) for i in block_raw_data['transactions']],
      "withdrawals": [dict(i) for i in block_raw_data['withdrawals']]
    }


  def limit_transactions(self, block_data, threshold):
    if threshold == 0: return block_data["transactions"]
    else: return block_data["transactions"][:threshold]
  

  def streaming_block_data(self, frequency):
    """Gera um stream de blocos minerados na rede EVM
    A cada frequência de clock, verifica se houve mineração de um novo bloco com uma requisição a API
    Gera dados do bloco em formato JSON se houve mineração de um novo bloco
    """
    previous_block = 0
    self.counter = 0
    while 1:
      actual_block = self.__get_latest_block()
      if not actual_block: continue
      if actual_block != previous_block:
        yield actual_block
        previous_block = actual_block
      time.sleep(float(frequency))


  def message_handler(self, err, msg):
    if err is not None: ErrorHandler(self.logger)(err)
    else: SuccessHandler(self.logger)(msg)


if __name__ == '__main__':
    
  APP_NAME = "BLOCK_CLOCK_APP"

  KAFKA_BROKERS = {'bootstrap.servers': os.getenv("KAFKA_BROKERS")} 
  SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
  TOPIC_LOGS = os.getenv("TOPIC_LOGS")
  TOPIC_BLOCK_METADATA = os.getenv("TOPIC_BLOCK_METADATA")
  TOPIC_TX_HASH_IDS = os.getenv("TOPIC_TX_HASH_IDS")
  TOPIC_TX_HASH_IDS_PARTITIONS = int(os.getenv("TOPIC_TX_HASH_IDS_PARTITIONS", 1))

  NETWORK = os.getenv("NETWORK")
  TXS_PER_BLOCK = int(os.getenv("TXS_PER_BLOCK", 8))
  CLOCK_FREQUENCY = float(os.getenv("CLOCK_FREQUENCY", 1))
  AKV_NAME = os.getenv('AKV_NAME')
  AKV_SECRET_NAME = os.getenv('AKV_SECRET_NAME')

  # Configuração de argumentos a serem passados via linha de comando e leitura do arquivo de configuração
  parser = argparse.ArgumentParser(description=f'Streaming de blocos minerados na rede {NETWORK}')
  parser.add_argument('config_producer', type=argparse.FileType('r'), help='Configurações de producers')
  args = parser.parse_args()
  config = ConfigParser()
  config.read_file(args.config_producer)

  AKV_URL = f'https://{AKV_NAME}.vault.azure.net/'
  AKV_CLIENT = SecretClient(vault_url=AKV_URL, credential=DefaultAzureCredential())

  # Configurando producers relativos a: block_metadata, hash_txs e logs
  create_simple_producer = lambda special_config: Producer(
    **KAFKA_BROKERS,
    **config['producer.general.config'],
    **config[special_config]
    )
  
  LOGS_PRODUCER = create_simple_producer('producer.logs.application')
  HASH_TXS_ID_PRODUCER = create_simple_producer('producer.hash_txs')
  
  # Configurando Logging
  LOGGER = logging.getLogger(APP_NAME)
  LOGGER.setLevel(logging.INFO)
  kafka_handler = KafkaLoggingHandler(LOGS_PRODUCER, TOPIC_LOGS)
  ConsoleLoggingHandler = ConsoleLoggingHandler()
  LOGGER.addHandler(ConsoleLoggingHandler)
  LOGGER.addHandler(kafka_handler)
  
  LOGGER.info(f"Starting {APP_NAME} application")
  LOGGER.info(f"NETWORK={NETWORK};TXS_PER_BLOCK={TXS_PER_BLOCK};CLOCK_FREQUENCY={CLOCK_FREQUENCY}")
  LOGGER.info(f"KAFKA_BROKERS={KAFKA_BROKERS}")
  LOGGER.info(f"TOPIC_LOGS={TOPIC_LOGS}")
  LOGGER.info(f"TOPIC_BLOCK_METADATA={TOPIC_BLOCK_METADATA}")
  LOGGER.info(f"TOPIC_TX_HASH_IDS={TOPIC_TX_HASH_IDS}")

  BLOCK_MINER = MinedBlocksProcessor(NETWORK, LOGGER, AKV_CLIENT, AKV_SECRET_NAME, SCHEMA_REGISTRY_URL)
  BLOCK_METADATA_PRODUCER = BLOCK_MINER.create_serializable_producer(
    {**KAFKA_BROKERS, **config['producer.general.config'], **config['producer.block_metadata']}
  )


  for raw_block_data in BLOCK_MINER.streaming_block_data(CLOCK_FREQUENCY):
    cleaned_block_data = BLOCK_MINER.parse_to_block_clock_schema(raw_block_data)
    key = str(cleaned_block_data['number'])
    BLOCK_METADATA_PRODUCER.produce(TOPIC_BLOCK_METADATA, key=key, value=cleaned_block_data, on_delivery=BLOCK_MINER.message_handler)
    BLOCK_METADATA_PRODUCER.poll(1)
    transactions = BLOCK_MINER.limit_transactions(cleaned_block_data, TXS_PER_BLOCK)
    for tx_hash_id_index in range(len(transactions)):
      partition = tx_hash_id_index % int(TOPIC_TX_HASH_IDS_PARTITIONS)
      message_key = transactions[tx_hash_id_index]
      HASH_TXS_ID_PRODUCER.produce(topic=TOPIC_TX_HASH_IDS, key=message_key, value=message_key, partition=partition)
    HASH_TXS_ID_PRODUCER.flush()
    BLOCK_METADATA_PRODUCER.flush()
    LOGGER.info(f"Kafka_Ingestion;TOPIC:{TOPIC_TX_HASH_IDS};NUM TRANSACTIONS:{len(transactions)};BLOCK NUMBER:{cleaned_block_data['number']}")
