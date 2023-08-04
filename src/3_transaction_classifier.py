import argparse, json, os
from streamer import ChainStreamer


class TransactionClassifier(ChainStreamer):

    # Método construtor da classe TransactionClassifier
    def __init__(self, network, to_cloud=False):
        self.network = network
        self.to_cloud = to_cloud


    # Método para classificar as transações e enviar para o Kafka ou EventHub
    def __classify_transaction(self, transaction) -> None:
        if transaction['input'] == '0x': 
            self._produce_data(transaction)
            return
        if transaction['to'] == None: 
            topic = f'{self.network}_contract_deployment'
            self.kafka_streamer.send_data(topic=topic, data=transaction)
            return
        else: 
            topic = f'{self.network}_contract_interaction'
            self.kafka_streamer.send_data(topic=topic, data=transaction)
            return


    def classify_transactions(self):
        for msg in self.kafka_streamer.consumer:
            tx = json.loads(msg.value)
            self.__classify_transaction(tx)


if __name__ == '__main__':

    network = os.environ["NETWORK"]
    eventhub_host = os.environ['EVENT_HUB_ENDPOINT']
    eventhub_name = f'{network}_{os.environ["EVENT_HUB_NAME"]}'
    kafka_host = os.environ["KAFKA_ENDPOINT"]
    topic_consume = f'{network}_{os.environ["TOPIC_CONSUME"]}'
    group_id = os.environ.get('CONSUMER_GROUP', 'group_1')
    to_cloud = True if os.environ.get('TO_CLOUD', False) == '1' else False

    tx_classifier = TransactionClassifier(network, to_cloud=to_cloud)
    tx_classifier.config_producer_kafka(connection_str=kafka_host, topic=f'{network}_contract_interaction', num_partitions=1)
    tx_classifier.config_consumer_kafka(connection_str=kafka_host, topic=topic_consume,consumer_group=group_id)
    tx_classifier.config_producer_event_hub(connection_str=eventhub_host, eventhub_name=eventhub_name)
    # python 3_transaction_classifier.py --topic_consumer raw_transactions --consumer_group group_1
    tx_classifier.classify_transactions()

  
