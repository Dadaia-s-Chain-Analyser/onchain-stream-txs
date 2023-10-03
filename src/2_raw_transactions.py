import argparse
import os, hexbytes
from web3 import Web3
from threading import Thread
from mother_class import MotherClass
from azure.identity import DefaultAzureCredential

from dadaia_tools.kafka_client import KafkaClient
from dadaia_tools.kafka_admin import KafkaAdminAPI
from dadaia_tools.azure_key_vault_client import KeyVaultAPI


class TransactionMiner(MotherClass):

    def __init__(self, network, api_key_node):
        self.network = network
        self.api_key_node = api_key_node
        vendor_url = self._get_url_node(network, api_key_node)
        self.web3 = Web3(Web3.HTTPProvider(vendor_url))


    def streaming_tx_data(self, topic_consumer):
        for msg in topic_consumer:
            tx_id = msg.value
            tx_data = self.web3.eth.get_transaction(tx_id)
            tx_data = {k: bytes.hex(v) if type(v) == hexbytes.main.HexBytes else v for k, v in tx_data.items()}
            if tx_data.get("accessList"):
                tx_data["accessList"] = [dict(i) for i in tx_data["accessList"]]
            yield tx_data
            


def thread_funct(thread_id, api_key, network, kafka_client, topic_consume, group_id, topic_produce):
    transaction_miner = TransactionMiner(network, api_key)
    producer = kafka_client.create_producer()
    consumer = kafka_client.create_consumer(consumer_group=group_id)
    consumer.subscribe([topic_consume])
    for data in transaction_miner.streaming_tx_data(consumer):
        kafka_client.send_data(producer, topic_produce, data)
        print(f"Process {thread_id}: transaction sent")


if __name__ == '__main__':

    network = os.environ["NETWORK"]
    kafka_host = os.environ["KAFKA_ENDPOINT"]
    key_vault_node_name = os.environ['KEY_VAULT_NODE_NAME']
    key_vault_node_secret = os.environ['KEY_VAULT_NODE_SECRET']

    parser = argparse.ArgumentParser(description=f'Stream transactions from {network} network')
    parser.add_argument('--topic_consume', required=False, type=str, help='Topic to consume', default="block_transactions")
    parser.add_argument('--consumer_group', required=False, type=str, help='Consumer Group', default="consumer-group-tx-1")
    parser.add_argument('--num_partitions', required=False, type=int, help='Number of partitions', default=1)
    parser.add_argument('--topic_produce', required=False, type=str, help='Topic to produce transaction data', default="raw_transactions")

    args = parser.parse_args()
    topic_consume = f'{network}_{args.topic_consume}'
    consumer_group = args.consumer_group
    num_partitions = args.num_partitions
    topic_produce = f'{network}_{args.topic_produce}'


    kafka_admin = KafkaAdminAPI(connection_str=kafka_host)
    kafka_admin.create_idempotent_topic(topic_name=topic_consume, topic_config={"num_partitions": num_partitions})
    kafka_admin.create_idempotent_topic(topic_name=topic_produce)


    credential = DefaultAzureCredential()
    key_vault_api = KeyVaultAPI(key_vault_node_name, credential)
    interval_keys = [int(i) for i in key_vault_node_secret.split("-")[-2:]]
    name_secret = "-".join(key_vault_node_secret.split("-")[:-2])
    api_keys = [key_vault_api.get_secret(f"{name_secret}-{i}") for i in range(interval_keys[0], interval_keys[1] + 1)]
    kafka_client = KafkaClient(connection_str=kafka_host)
    thread_funct_fixed_args = [network, kafka_client, topic_consume, consumer_group, topic_produce]

    for thread_id in range(len(api_keys)):
        thread = Thread(target=thread_funct, args=(thread_id + 1, api_keys[thread_id], *thread_funct_fixed_args))
        thread.start()

