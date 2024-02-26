import argparse, json, os
import logging
from configparser import ConfigParser
from confluent_kafka import Consumer, Producer
from dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler


class TransactionClassifier:

    # Método construtor da classe TransactionClassifier
    def __init__(self, network):
        self.network = network


    # Método para classificar as transações e enviar para o Kafka ou EventHub
    def __classify_transaction(self, transaction) -> None:
       
        if transaction['input'] == '': 
            topic = f'{self.network}_simple_transaction'
        elif transaction['to'] == None:
            topic = f'{self.network}_contract_deployment'
        else: 
            topic = f'{self.network}_contract_interaction'
        return topic, transaction 

    def consuming_topic(self, consumer):
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg: yield json.loads(msg.value())


    def process_msg(self, msg):

        contract_deployment = lambda tx: tx['to'] == None
        token_transfer = lambda tx: tx['input'] == ''
        topic = 0 if token_transfer(msg) else 1 if contract_deployment(msg) else 2
        return msg, topic
            
            # topic, transaction = self.__classify_transaction(tx)
            # yield topic, transaction



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
    list_topics = [topic_out_nt_transfer, topic_out_sc_interaction, topic_out_sc_deployment]
    try:
        for msg in tx_classifier.consuming_topic(consumer_txs_hash):
            tx_data, topic_id = tx_classifier.process_msg(msg)
            encoded_message = json.dumps(tx_data).encode('utf-8')
            producer_classified_txs.produce(list_topics[topic_id], value=encoded_message)
            producer_classified_txs.flush()
            logger.info(f"tx_classified;{tx_data['hash'][:16]};topic={list_topics[topic_id]}")
            
    finally: consumer_txs_hash.close()


    # kafka_client = KafkaClient(connection_str=kafka_host)
    # producer = kafka_client.create_producer()
    # consumer = kafka_client.create_consumer(consumer_group=group_id)
    # consumer.subscribe([topic_consume])
    # for topic, transaction in tx_classifier.classify_transactions(consumer):
    #     kafka_client.send_data(producer, topic, transaction)
    #     print(f"Transaction sent to {topic}")

  
