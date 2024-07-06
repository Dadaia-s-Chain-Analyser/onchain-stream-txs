import argparse
import datetime as dt
import hexbytes
import json
import logging
import os
import time
from configparser import ConfigParser
from requests import HTTPError
from dm_utilities.dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler
from dm_utilities.dm_BNaaS_connector import BlockchainNodeAsAServiceConnector
from confluent_kafka import Producer
from dadaia_tools.azure_key_vault_client import KeyVaultAPI
from azure.identity import DefaultAzureCredential


class MinedBlocksProcessor(BlockchainNodeAsAServiceConnector):

  def __init__(self, network, logger, api_key_node, akv_secret_name):
    self.logger = logger
    self.kv_secret_name = akv_secret_name
    self.web3 = self.get_node_connection(network, api_key_node, 'alchemy')


  def __get_latest_block(self):
    try:
      block_info = self.web3.eth.get_block('latest')
      self.logger.info(f"API_request;{self.kv_secret_name}")
      return block_info
    except HTTPError as e:
      self.logger.error(f"API_request;{self.kv_secret_name};Error:{str(e)}")
      if str(e)[:3] == '429': return
      return

  def __parse_block_data(self, block):
    block_data = {k: v for k, v in block.items() if k not in ('transactions', 'withdrawals')}
    transactions = [bytes.hex(i) for i in block['transactions']]
    block_data['transactions'] = transactions
    block_data = {k: bytes.hex(v) if type(v) == hexbytes.main.HexBytes else v for k, v in block_data.items()}
    withdrawals = [dict(i) for i in block['withdrawals']]
    block_data['withdrawals'] = withdrawals
    block_data['totalDifficulty'] = str(block_data['totalDifficulty'])
    return block_data

  def limit_transactions(self, block_data, threshold):
    """Limita o número de transações por bloco de acordo com o valor de tx_threshold
    Usado para limitar o número de requisições a API em testes
    """
    return block_data["transactions"] if threshold == 0 \
                    else block_data["transactions"][:threshold]
  

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
        block_data = self.__parse_block_data(actual_block)
        yield block_data
        previous_block = actual_block
      time.sleep(float(frequency))


if __name__ == '__main__':
    
  # Leitura de variáveis de ambiente
  network = os.environ["NETWORK"]
  akv_node_name = os.environ['KEY_VAULT_NODE_NAME']
  akv_secret_name = os.environ['KEY_VAULT_NODE_SECRET']
  KAFKA_BROKER = os.getenv("KAFKA_CLUSTER", "broker:29092")
  bootstrap_servers = {'bootstrap.servers': KAFKA_BROKER}
  
  # Configuração de argumentos a serem passados via linha de comando e leitura do arquivo de configuração
  parser = argparse.ArgumentParser(description=f'Streaming de blocos minerados na rede EVM {network}')
  parser.add_argument('config_producer', type=argparse.FileType('r'), help='Configurações de producers')
  parser.add_argument('--frequency', type=float, help='Frequência de clock', default=1)
  parser.add_argument('--tx_threshold', type=int, help='Limite transações por bloco', default=8)
  args = parser.parse_args()
  config = ConfigParser()
  config.read_file(args.config_producer)

  # Obtendo API Key do Azure Key Vault
  credential = DefaultAzureCredential()
  key_vault_api = KeyVaultAPI(akv_node_name, credential)
  api_key_node = key_vault_api.get_secret(akv_secret_name)

  # Configurando producers relativos a: block_metadata, hash_txs e logs
  create_producer = lambda special_config: Producer(**bootstrap_servers, **config['producer.general.config'], **config[special_config])
  producer_block_metadata = create_producer('producer.block_metadata')
  producer_hash_txs = create_producer('producer.hash_txs')
  producer_logs = create_producer('producer.logs.application')

  # Obter nome dos tópicos a partir do arquivo de configuração
  make_topic_name = lambda topic: f"{network}.{dict(config[topic])['topic']}"
  topic_out_logs = make_topic_name('topic.app.logs')
  topic_out_block_metadata = make_topic_name('topic.block_metadata')
  topic_out_block_hash_txs = make_topic_name('topic.hash_txs')
  topic_num_partitions_hash_txs = dict(config['topic.hash_txs'])['num_partitions']
  
  # Configurando Logging
  logger = logging.getLogger("app-block-clock")
  logger.setLevel(logging.INFO)
  kafka_handler = KafkaLoggingHandler(producer_logs, topic_out_logs)
  ConsoleLoggingHandler = ConsoleLoggingHandler()
  logger.addHandler(ConsoleLoggingHandler)
  logger.addHandler(kafka_handler)

  # Inicializa processo de streaming com Source = mined block event -> process -> Sinks = (block_metadata,raw_txs)
  block_miner = MinedBlocksProcessor(network, logger, api_key_node, akv_secret_name)

  for block_data in block_miner.streaming_block_data(args.frequency):
    encoded_message = json.dumps(block_data).encode('utf-8')
    producer_block_metadata.produce(topic_out_block_metadata, value=encoded_message)
    producer_block_metadata.flush()
    logger.info(f"mined_block_ingested;{len(block_data['transactions'])};{block_data['number']}")
    counter = 0
    transactions = block_miner.limit_transactions(block_data, args.tx_threshold)
    for tx_data in transactions:
      partition = counter % int(topic_num_partitions_hash_txs)
      counter += 1
      counter = 0 if counter == int(topic_num_partitions_hash_txs) else counter
      producer_hash_txs.produce(topic=topic_out_block_hash_txs, value=tx_data, partition=partition)
    producer_hash_txs.flush()
    logger.info(f"hash_id_txs_ingested;{len(transactions)};{block_data['number']}")
