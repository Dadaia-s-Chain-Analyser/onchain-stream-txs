import json, os, argparse
from functools import lru_cache
from mother_class import MotherClass
from web3 import Web3
from apis.kafka_api import KafkaClient
from apis.azure_key_vault_api import KeyVaultAPI
from azure.identity import DefaultAzureCredential


class TransactionConverter(MotherClass):


  def __init__(self, network, api_key_node, api_key_scan):
    self.network = network
    self.api_key_node = api_key_node
    self.api_key_scan = api_key_scan

    # vendor_url = self.get_node_url(network, api_key_node)
    vendor_url = self._get_url_node(network, api_key_node)
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


  def convert_input_transactions(self, topic_consumer):

    for msg in topic_consumer:
      detail_transaction = json.loads(msg.value)
      contract_address = detail_transaction['to']
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
    kafka_host = os.environ["KAFKA_ENDPOINT"]
    key_vault_node_name = os.environ['KEY_VAULT_NODE_NAME']
    key_vault_node_secret = os.environ['KEY_VAULT_NODE_SECRET']
    key_vault_scan_name = os.environ['KEY_VAULT_SCAN_NAME']
    key_vault_scan_secret = os.environ['KEY_VAULT_SCAN_SECRET']  

    argparser = argparse.ArgumentParser(description=f'Stream transactions from {network} network')

    argparser.add_argument('--topic_consume', required=False, type=str, help='Topic to consume', default="contract_interaction")
    argparser.add_argument('--consumer_group', required=False, type=str, help='Consumer Group', default="consumer-group-tx-2")

    args = argparser.parse_args()
    topic_consume = f'{network}_{args.topic_consume}'
    group_id = args.consumer_group


    credential = DefaultAzureCredential()
    key_vault_scan_api = KeyVaultAPI(key_vault_scan_name, credential)
    key_vault_node_api = KeyVaultAPI(key_vault_node_name, credential)

    api_key_scan = key_vault_scan_api.get_secret(key_vault_scan_secret)
    api_key_node = key_vault_node_api.get_secret(key_vault_node_secret)

    kafka_client = KafkaClient(connection_str=kafka_host)

    consumer = kafka_client.create_consumer(topic=topic_consume, consumer_group=group_id)

    tx_converter = TransactionConverter(network, api_key_node, api_key_scan)
    tx_converter.convert_input_transactions(consumer)


