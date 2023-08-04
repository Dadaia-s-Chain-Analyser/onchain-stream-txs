import json, argparse
import os, hexbytes
from web3 import Web3
from utils.utils import get_node_url
from streamer import ChainStreamer
from threading import Thread


class TransactionMiner(ChainStreamer):

    def __init__(self, network, api_key_node):
        self.network = network
        self.api_key_node = api_key_node
        vendor_url = get_node_url(network, api_key_node)
        self.web3 = Web3(Web3.HTTPProvider(vendor_url))


    def streaming_tx_data(self, thread_id, topic_output):
        for msg in self.kafka_streamer.consumer:
            tx_id = json.loads(msg.value)
            tx_data = self.web3.eth.get_transaction(tx_id)
            tx_data = {k: bytes.hex(v) if type(v) == hexbytes.main.HexBytes else v for k, v in tx_data.items()}
            if tx_data.get("accessList"):
                tx_data["accessList"] = [dict(i) for i in tx_data["accessList"]]
            self.kafka_streamer.producer.send(topic=topic_output, value=tx_data)
            print(f"Thread {thread_id} - Transaction {tx_id} sent to topic {topic_output}")

    
def thread_funct(thread_id, api_key, network, kafka_host, topic_consume, group_id, topic_produce):
    transaction_miner = TransactionMiner(network, api_key)
    transaction_miner.config_producer_kafka(connection_str=kafka_host, topic=topic_produce)
    transaction_miner.config_consumer_kafka(connection_str=kafka_host, topic=topic_consume,consumer_group=group_id)
    transaction_miner.streaming_tx_data(thread_id, topic_produce)


# This function creates a thread with the given thread_id and fixed arguments
def create_thread(thread_id, *fixed_args):
    api_keys = list(filter(lambda x: x[:12] == "NODE_API_KEY", os.environ))
    api_keys = [os.environ[i] for i in api_keys]
    return Thread(target=thread_funct, args=(thread_id, api_keys[thread_id-1], *fixed_args))


if __name__ == '__main__':

    network = os.environ["NETWORK"]
    kafka_host = os.environ["KAFKA_ENDPOINT"]
    
   
    topic_consume = f'{network}_{os.environ["TOPIC_CONSUME"]}'
    topic_produce = f'{network}_{os.environ["TOPIC_PRODUCE"]}'
    num_partitions = int(os.environ.get('NUM_PARTITIONS', 1))
    consumer_group = os.environ.get('CONSUMER_GROUP', 'group_1')

    thread_funct_fixed_args = [network, kafka_host, topic_consume, consumer_group, topic_produce]
    
    for thread_id in range(1, num_partitions + 1):
        create_thread(thread_id, *thread_funct_fixed_args).start()
