import argparse, os, logging
from datetime import datetime
from azure.identity import DefaultAzureCredential
from apis.kafka_api import KafkaClient
from apis.etherscan_api import EthercanAPI
from apis.azure_key_vault_api import KeyVaultAPI


class BatchContractTransactions:

    def __init__(self, api_key, network, contract_address):
        self.contract_address = contract_address
        self.etherscan_api = EthercanAPI(api_key, network)


    def get_block_interval(self, start_date, end_date=None):
        start_date, end_date = int(datetime.timestamp(start_date)),  int(datetime.timestamp(end_date))
        url_start_block = self.etherscan_api.get_block_by_time_url(timestamp=start_date, closest='after')
        url_end_block = self.etherscan_api.get_block_by_time_url(timestamp=end_date, closest='before')
        block_bottom = self.etherscan_api.req_chain_scan(url_start_block)
        block_top = self.etherscan_api.req_chain_scan(url_end_block)
        return int(block_bottom),int(block_top)


    def __get_transactions(self, startblock, endblock, batch_size=10000):
        url_start_block = self.etherscan_api.get_txlist_url(self.contract_address, startblock, endblock, offset=batch_size)
        list_transactions = self.etherscan_api.req_chain_scan(url_start_block)
        return list_transactions
    
  
    def open_channel_txs(self, start_date, end_date=None):
        block_bottom, block_top = self.get_block_interval(start_date, end_date=end_date)
        print(block_bottom, block_top)
        for data in self.batch_contract_txs(block_bottom, block_top):
            yield data


    def batch_contract_txs(self, block_bottom, block_top):
        while 1:
            list_gross_tx = self.__get_transactions(block_bottom, block_top)
            logging.info(f"Blocks to be analysed: {block_top - block_bottom}")
            next_bottom = int(list_gross_tx[-1]['blockNumber'])
            block_bottom = next_bottom
            yield list_gross_tx
            if (block_top - next_bottom < 100) and (len(list_gross_tx) <= 1): break
            if list_gross_tx == False: return "COMPLETED"
        return "COMPLETED"
    

if __name__ == '__main__':

    network = os.environ['NETWORK']
    contract_address = os.environ['CONTRACT']
    kafka_host = os.environ['KAFKA_ENDPOINT']
    topic_produce = f'{network}_{os.environ["TOPIC_PRODUCE"]}'
    key_vault_scan_name = os.environ['KEY_VAULT_SCAN_NAME']
    key_vault_scan_secret = os.environ['KEY_VAULT_SCAN_SECRET']

    credential = DefaultAzureCredential()
    key_vault_api = KeyVaultAPI(key_vault_scan_name, credential)
    api_key = key_vault_api.get_secret(key_vault_scan_secret)

    parser = argparse.ArgumentParser(description='Batch Smart Contract Transactions')
    parser.add_argument('--start_date', type=str, help='Start Date', default=None)
    parser.add_argument('--end_date', type=str, help='End Date', default=None)

    args = parser.parse_args()
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else datetime.now()

    batch_contract_txs = BatchContractTransactions(api_key, network, contract_address)
    kafka_client = KafkaClient(connection_str=kafka_host)
    producer = kafka_client.create_producer()

    counter = 0
    for transaction in batch_contract_txs.open_channel_txs(start_date, end_date):
        for i in transaction:
            counter += 1
            if counter % 1000 == 0:
                print(f"Transactions processed: {counter}")
            producer.send(topic_produce, i)

