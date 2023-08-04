import time, os
import hexbytes
from requests import HTTPError
from web3 import Web3
from streamer import ChainStreamer
from utils.utils import get_node_url


class BlockMiner(ChainStreamer):

    def __init__(self, network, api_key_node, to_cloud=False, tx_threshold=None, frequency=1):
        self.to_cloud = to_cloud
        self.tx_threshold = tx_threshold
        self.frequency = float(frequency)
        self.configure_blockchain_connection(network, api_key_node)


    def configure_blockchain_connection(self, network, api_key_node):
        vendor_url = get_node_url(network, api_key_node, 'alchemy')
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


    def __produce_transaction_data(self, block_data):
        if self.tx_threshold == 0: transactions = block_data["transactions"]
        else: transactions = block_data["transactions"][:self.tx_threshold]
        print
        for tx_data in transactions:
            self.counter += 1
            partition = self.counter % self.num_partitions
            self.kafka_streamer.send_data(self.topic_produce, tx_data, partition, f"key_{partition}")
        print(f"Transactions from block {block_data['number']} sent through kafka")


    def streaming_block_data(self):
        previous_block = 0
        self.counter = 0
        while 1:
            actual_block = self.__get_latest_block()
            if not actual_block: continue
            if actual_block != previous_block:
                block_data = self.__parse_block_data(actual_block)
                self._produce_data(block_data)
                print(f"Block {block_data['number']} sent!")
                self.__produce_transaction_data(block_data)
                previous_block = actual_block
            time.sleep(float(self.frequency))


if __name__ == '__main__':

    frequency = float(os.environ.get('FREQUENCY', 1))
    to_cloud = True if os.environ.get('TO_CLOUD', False) == '1' else False
    api_key_node = os.environ['NODE_API_KEY']
    network = os.environ["NETWORK"]
    eventhub_host = os.environ['EVENT_HUB_ENDPOINT']
    eventhub_name = f"{network}_{os.environ['EVENT_HUB_NAME']}"
    kafka_host = os.environ['KAFKA_ENDPOINT']
    topic_produce = f"{network}_{os.environ['TOPIC_PRODUCE']}"
    num_partitions = int(os.environ.get('NUM_PARTITIONS', 1))
    tx_threshold=int(os.environ.get('THRESHOLD', 0))

    block_miner = BlockMiner(network, api_key_node, to_cloud, tx_threshold, frequency)
    block_miner.config_producer_kafka(connection_str=kafka_host, topic=topic_produce, num_partitions=num_partitions)
    block_miner.config_producer_event_hub(connection_str=eventhub_host, eventhub_name=eventhub_name)
    block_miner.streaming_block_data()
