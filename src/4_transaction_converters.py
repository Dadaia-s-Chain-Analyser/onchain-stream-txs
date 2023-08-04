import json, os, argparse
from functools import lru_cache
from web3 import Web3
from utils.utils import get_node_url
from streamer import ChainStreamer



class TransactionConverter(ChainStreamer):


  def __init__(self, network, api_key_node, api_key_scan):
    self.network = network
    self.api_key_node = api_key_node
    self.api_key_scan = api_key_scan
    vendor_url = get_node_url(network, api_key_node, "alchemy")
    self.web3 = Web3(Web3.HTTPProvider(vendor_url))


  @lru_cache(maxsize=None)
  def _get_contract(self, address, abi):
    """
    This helps speed up execution of decoding across a large dataset by caching the contract object
    It assumes that we are decoding a small set, on the order of thousands, of target smart contracts
    """
    if isinstance(abi, (str)):
      abi = json.loads(abi)

    contract = self.web3.eth.contract(address=address, abi=abi)
    return (contract, abi)


  def convert_input_transactions(self):

    for msg in self.kafka_streamer.consumer:
      detail_transaction = json.loads(msg.value)
      contract_address = detail_transaction['to']
      self.event_hub_streamer.send_data(detail_transaction)
      print(f"Sent transaction {detail_transaction['hash']} to eventhub")
      # try: 
      #   abi = req_chain_scan(self.api_key_scan, get_abi_url, dict(address=contract_address))
    
      #   contract, _ = self._get_contract(address=contract_address, abi=abi)
      # except:
      #   print(detail_transaction)
      #   continue
      # try: method, parms_method = contract.decode_function_input(detail_transaction['input'])
      # except: parms_method = detail_transaction['input']
      # else:
      #   for key in parms_method:
      #     if isinstance(parms_method[key], (bytes)): parms_method[key] = parms_method[key].hex()
      #     elif isinstance(parms_method[key], (list)): parms_method[key] = [x.hex() if isinstance(x, (bytes)) else x for x in parms_method[key]]
      #     elif isinstance(parms_method[key], (dict)): parms_method[key] = {k.hex() if isinstance(k, (bytes)) else k: v.hex() if isinstance(v, (bytes)) else v for k, v in parms_method[key].items()}
      
      # payload = dict(method=str(method), parms=parms_method)
      # detail_transaction['input'] = payload

      # self.event_hub_streamer.send_data(detail_transaction)
      # print(f"Sent transaction {detail_transaction['hash']} to eventhub")

  def convert_transactions(self):
    pass
    # web3 = Web3(Web3.HTTPProvider(f'https://{network}.infura.io/v3/{api_key_node}'))


  
if __name__ == '__main__':
    network = os.environ["NETWORK"]
    api_key_node = os.environ['NODE_API_KEY']
    api_key_scan = os.environ['SCAN_API_KEY']  
    kafka_host = os.environ["KAFKA_ENDPOINT"]
    eventhub_host = os.environ['EVENT_HUB_ENDPOINT']

    
    parser = argparse.ArgumentParser(description='Transaction Converter')
    parser.add_argument('--topic_consume', type=str, help='Kafka topic to consume', default=f'{network}_contract_interaction')
    parser.add_argument('--consumer_group', type=str, help='Kafka consumer group', default='tx_converter')
    parser.add_argument('--eventhub_name', type=str, help='Name of the eventhub', default='contract_transactions')

  # python 4_transaction_converters.py --topic_consume=bsc_contract_interaction --consumer_group=tx_converter --eventhub_name=contract_transactions
    args = parser.parse_args()
    topic_consume = f'{network}_{args.topic_consume}'
    eventhub_name = f'{network}_{args.eventhub_name}'
    consumer_group = args.consumer_group
    

    tx_converter = TransactionConverter(network, api_key_node, api_key_scan)
    tx_converter.config_consumer_kafka(connection_str=kafka_host, topic=topic_consume,consumer_group=consumer_group)
    tx_converter.config_producer_event_hub(connection_str=eventhub_host, eventhub_name=eventhub_name)

    tx_converter.convert_input_transactions()


