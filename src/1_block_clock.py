import time, os, argparse, hexbytes
from requests import HTTPError
from web3 import Web3
from apis.kafka_api import KafkaClient
from apis.azure_key_vault_api import KeyVaultAPI
from azure.identity import DefaultAzureCredential

class BlockMiner:

    def __init__(self, network, api_key_node, tx_threshold=None, frequency=1):
        self.tx_threshold = tx_threshold
        self.frequency = float(frequency)
        self.configure_blockchain_connection(network, api_key_node)


    def get_node_url(self, network, api_key, vendor="infura"):
        dict_vendors = { 
            'alchemy': f"https://eth-{network}.g.alchemy.com/v2/{api_key}",
            'infura': f"https://{network}.infura.io/v3/{api_key}"
        }
        return dict_vendors.get(vendor)


    def configure_blockchain_connection(self, network, api_key_node):
        vendor_url = self.get_node_url(network, api_key_node, 'alchemy')
        self.web3 = Web3(Web3.HTTPProvider(vendor_url))


    def __get_latest_block(self):
        try:
            block_info = self.web3.eth.get_block('latest')
            return block_info
        except HTTPError as e:
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


    def streaming_block_data(self):
        previous_block = 0
        self.counter = 0
        while 1:
            actual_block = self.__get_latest_block()
            if not actual_block: continue
            if actual_block != previous_block:
                block_data = self.__parse_block_data(actual_block)
                
                yield block_data
                previous_block = actual_block
            time.sleep(float(self.frequency))


if __name__ == '__main__':

    network = os.environ["NETWORK"]
    kafka_host = os.environ['KAFKA_ENDPOINT']
    key_vault_node_name = os.environ['KEY_VAULT_NODE_NAME']
    key_vault_node_secret = os.environ['KEY_VAULT_NODE_SECRET']

    parser = argparse.ArgumentParser(description=f'Stream Blocks from {network} network')
    parser.add_argument('--frequency', type=float, help='Clock Frequency', default=1)
    parser.add_argument('--tx_threshold', type=int, help='Transaction Threshold', default=0)
    parser.add_argument('--num_partitions', type=int, help='Number of Partitions', default=1)
    parser.add_argument('--topic_blocks', type=str, help='Topic to produce block data', default="mined_blocks")
    parser.add_argument('--topic_transactions', type=str, help='Topic to produce transaction data', default="block_transactions")

    args = parser.parse_args()
    frequency = args.frequency
    tx_threshold = args.tx_threshold
    topic_blocks = f'{network}_{args.topic_blocks}'
    topic_transactions = f'{network}_{args.topic_transactions}'
    num_partitions = args.num_partitions

    credential = DefaultAzureCredential()
    key_vault_api = KeyVaultAPI(key_vault_node_name, credential)
    api_key_node = key_vault_api.get_secret(key_vault_node_secret)

    kafka_client = KafkaClient(connection_str=kafka_host)
    block_miner = BlockMiner(network, api_key_node, tx_threshold, frequency)
    kafka_client.create_idempotent_topic(topic=topic_blocks)
    kafka_client.create_idempotent_topic(topic=topic_transactions, num_partitions=num_partitions)
    producer = kafka_client.create_producer()

    for block_data in block_miner.streaming_block_data():
        print(f"Block {block_data['number']} sent!")
        kafka_client.send_data(producer, topic_blocks, block_data)
        counter = 0
        if tx_threshold == 0: transactions = block_data["transactions"]
        else: transactions = block_data["transactions"][:tx_threshold]
        for tx_data in transactions:
            partition = counter % num_partitions
            counter += 1
            kafka_client.send_data(producer, topic_transactions, tx_data, partition, f"key_{partition}")
        print(f"Transactions from block {block_data['number']} sent through kafka")