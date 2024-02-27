import argparse
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

from dm_utils import DataMasterUtils
from dm_BNaaS_connector import BlockchainNodeAsAServiceConnector
from dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler


class LogProcessor(BlockchainNodeAsAServiceConnector):

    
  def consuming_topic(self, consumer):
    while True:
      msg = consumer.poll(timeout=0.1)
      if msg: 
        yield msg.value().decode('utf-8')


  def process_msg(self, msg):
    return msg


# subscreve em todos os topicos de logs relativos a API KEYs
# e verifica se todas as API KEYs estão cadastradas no Kafka


# escreve API KEY usada em requisição, numero de requisições no ultimo dia e timestamp
# escreve em tópico compacto de API_KEYs

if __name__ == "__main__":

  network = os.environ["NETWORK"]
  parser = argparse.ArgumentParser(description=f'Login Processor')
  parser.add_argument('config_producer', type=argparse.FileType('r'), help='Config Producers')
  parser.add_argument('config_consumer', type=argparse.FileType('r'), help='Config Consumers')
  args = parser.parse_args()

  # Parse de dados provenientes dos arquivos de configuração para variável config
  config = ConfigParser()
  config.read_file(args.config_producer)
  config.read_file(args.config_consumer)


  # Nome de tópicos provenientes do arquivo de configuração
  make_topic_name = lambda topic: f"{network}.{dict(config[topic])['topic']}"
  topic_in_logs = make_topic_name('topic.app.logs')

  consumer_txs_sc_int = Consumer(**config['consumer.general.config'], **config['consumer.app.logs'])
  consumer_txs_sc_int.subscribe([topic_in_logs])

  log_processor = LogProcessor()

  for log_data in log_processor.consuming_topic(consumer_txs_sc_int):
    print(log_data)