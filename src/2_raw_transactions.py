import json, argparse
import os, hexbytes
from web3 import Web3
from utils.utils import get_node_url
from pub_sub_api import KafkaClient
from threading import Thread


class TransactionMiner:

    def __init__(self, network, api_key_node):
        self.network = network
        self.api_key_node = api_key_node
        vendor_url = get_node_url(network, api_key_node)
        self.web3 = Web3(Web3.HTTPProvider(vendor_url))


    def streaming_tx_data(self, topic_consumer):
        for msg in topic_consumer:
            tx_id = json.loads(msg.value)
            tx_data = self.web3.eth.get_transaction(tx_id)
            tx_data = {k: bytes.hex(v) if type(v) == hexbytes.main.HexBytes else v for k, v in tx_data.items()}
            if tx_data.get("accessList"):
                tx_data["accessList"] = [dict(i) for i in tx_data["accessList"]]
            yield tx_data
            

    
def thread_funct(thread_id, api_key, network, kafka_client, topic_consume, group_id, topic_produce):
    transaction_miner = TransactionMiner(network, api_key)
    kafka_client.create_idempotent_topic(topic=topic_consume, num_partitions=num_partitions)
    producer = kafka_client.create_producer()
    consumer = kafka_client.create_consumer(topic=topic_consume, consumer_group=group_id)
    for data in transaction_miner.streaming_tx_data(consumer):
        kafka_client.send_data(producer, topic_produce, data)
        print(f"Process {thread_id}: transaction sent")


if __name__ == '__main__':

    network = os.environ["NETWORK"]
    kafka_host = os.environ["KAFKA_ENDPOINT"]
    topic_consume = f'{network}_{os.environ["TOPIC_CONSUME"]}'
    topic_produce = f'{network}_{os.environ["TOPIC_PRODUCE"]}'
    consumer_group = os.environ.get('CONSUMER_GROUP', 'group_1')
    api_keys = filter(lambda x: x[:12] == "NODE_API_KEY", os.environ)
    api_keys = list(map(lambda x: os.environ[x], api_keys))
    kafka_client = KafkaClient(connection_str=kafka_host)
    num_partitions = len(api_keys)
    kafka_client.create_idempotent_topic(topic=topic_consume, num_partitions=num_partitions)
    kafka_client.create_idempotent_topic(topic=topic_produce)

    thread_funct_fixed_args = [network, kafka_client, topic_consume, consumer_group, topic_produce]
    for thread_id in range(num_partitions):
        thread = Thread(target=thread_funct, args=(thread_id + 1, api_keys[thread_id], *thread_funct_fixed_args))
        thread.start()

