import argparse, json, os
import logging
from configparser import ConfigParser
from confluent_kafka import Consumer, Producer
from dm_utilities.dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler


class TransactionClassifier:

  def __init__(self, network):
    self.network = network

  def consuming_topic(self, consumer):
    while True:
      msg = consumer.poll(timeout=0.1)
      if msg: 
        yield json.loads(msg.value())

  def process_msg(self, msg, map_topics):
    if msg['to'] == None: topic = '1'
    elif msg['input'] == '': topic = '2'
    else: topic = '3'
    return msg, map_topics[topic]



if __name__ == '__main__':

  network = os.environ["NETWORK"]

  parser = argparse.ArgumentParser(description=f'Stream cleaned transactions network')
  parser.add_argument('config_producer', type=argparse.FileType('r'), help='Config Producers')
  parser.add_argument('config_consumer', type=argparse.FileType('r'), help='Config Consumers')
  args = parser.parse_args()

  # Parse de dados provenientes dos arquivos de configuração para variável config
  config = ConfigParser()
  config.read_file(args.config_producer)
  config.read_file(args.config_consumer)
  
  # Nome de tópicos provenientes do arquivo de configuração
  make_topic_name = lambda topic: f"{network}.{dict(config[topic])['topic']}"
  topic_in_raw_txs = make_topic_name('topic.raw_txs')
  topic_out_nt_transfer = make_topic_name('topic.tx.native_token_transfer')
  topic_out_sc_interaction = make_topic_name('topic.tx.contract_interaction')
  topic_out_sc_deployment = make_topic_name('topic.tx.contract_deployment')
  topic_out_logs = make_topic_name('topic.app.logs')

  # Criação de 1 consumer para o tópico de hash_txs e 1 producer para o tópico de raw_txs
  create_producer = lambda special_config: Producer(**config['producer.general.config'], **config[special_config])
  producer_logs = create_producer('producer.logs.block_clock')
  producer_classified_txs = create_producer('producer.classified_txs')
  consumer_txs_hash = Consumer(**config['consumer.general.config'], **config['consumer.raw_txs'])
  consumer_txs_hash.subscribe([topic_in_raw_txs])

  # Configurando Logging para console e Kafka
  logger = logging.getLogger("transaction-classifier")
  
  logger.setLevel(logging.INFO)
  kafka_handler = KafkaLoggingHandler(producer_logs, topic_out_logs)
  ConsoleLoggingHandler = ConsoleLoggingHandler()
  logger.addHandler(ConsoleLoggingHandler)
  logger.addHandler(kafka_handler)

  # Inicializa processo de streaming com Source = mined block event -> process -> Sinks = (block_metadata,raw_txs)
  tx_classifier = TransactionClassifier(network)
  map_topics = {'1': topic_out_sc_deployment, '2': topic_out_nt_transfer, '3': topic_out_sc_interaction}
  try:
    for msg in tx_classifier.consuming_topic(consumer_txs_hash):
      tx_data, topic = tx_classifier.process_msg(msg, map_topics)
      encoded_message = json.dumps(msg).encode('utf-8')
      producer_classified_txs.produce(topic, value=encoded_message)
      producer_classified_txs.flush()
      logger.info(f"raw_transaction_classified;{msg['hash'][:16]};topic={topic}")
  finally: consumer_txs_hash.close()