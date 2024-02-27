import json, os, argparse
from functools import lru_cache
from json.decoder import JSONDecodeError
from web3 import Web3
from confluent_kafka import Producer, Consumer
from dadaia_tools.azure_key_vault_client import KeyVaultAPI
from dadaia_tools.etherscan_client import EthercanAPI
from azure.identity import DefaultAzureCredential
from configparser import ConfigParser
from dm_utils import DataMasterUtils
from dm_BNaaS_connector import BlockchainNodeAsAServiceConnector
from dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler


class TransactionConverter(BlockchainNodeAsAServiceConnector):


  def __init__(self, network, api_key_node, ethercan_api):
    self.network = network
    self.etherscan_api = ethercan_api
    self.web3 = self._get_node_connection(network, api_key_node, 'alchemy')


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


  def decode_input(self, contract_address, input_data):
    try: 
      abi = self.etherscan_api.get_contract_abi(contract_address)['result']
      contract, _ = self._get_contract(address=contract_address, abi=abi)
      method, parms_method = contract.decode_function_input(input_data)
    except TypeError as e: print("Deu Ruim")
    except JSONDecodeError as e: print(e)
    except ValueError as e: print(e)
    else:
      for key in parms_method:
        if isinstance(parms_method[key], (bytes)): parms_method[key] = parms_method[key].hex()
        elif isinstance(parms_method[key], (list)): parms_method[key] = [x.hex() if isinstance(x, (bytes)) else x for x in parms_method[key]]
        elif isinstance(parms_method[key], (dict)): parms_method[key] = {k.hex() if isinstance(k, (bytes)) else k: v.hex() if isinstance(v, (bytes)) else v for k, v in parms_method[key].items()}
        method = str(method).split('<Function ')[1].split('>')[0]
        input_data = dict(method=method, parms=parms_method)
        return input_data

  def consuming_topic(self, consumer):
    while True:
      msg = consumer.poll(timeout=0.1)
      if msg: yield json.loads(msg.value())

        

if __name__ == '__main__':
    
  network = os.environ["NETWORK"]
  key_vault_node_name = os.environ['KEY_VAULT_NODE_NAME']
  key_vault_node_secret = os.environ['KEY_VAULT_NODE_SECRET']
  key_vault_scan_name = os.environ['KEY_VAULT_SCAN_NAME']
  key_vault_scan_secret = os.environ['KEY_VAULT_SCAN_SECRET']
  credential = DefaultAzureCredential()
  key_vault_scan_api = KeyVaultAPI(key_vault_scan_name, credential)
  key_vault_node_api = KeyVaultAPI(key_vault_node_name, credential)
  api_key_scan = key_vault_scan_api.get_secret(key_vault_scan_secret)
  api_key_node = key_vault_node_api.get_secret(key_vault_node_secret)

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
  topic_in_sc_interaction = make_topic_name('topic.tx.contract_interaction')
  topic_out_parsed_input = make_topic_name('topic.txs.input.decoded')
  topic_out_logs = make_topic_name('topic.app.logs')

  # Criação de 1 consumer para o tópico de hash_txs e 1 producer para o tópico de raw_txs
  create_producer = lambda special_config: Producer(**config['producer.general.config'], **config[special_config])
  producer_logs = create_producer('producer.logs.block_clock')
  producer_parsed_input_txs = create_producer('producer.txs.input.decoded')
  consumer_txs_sc_int = Consumer(**config['consumer.general.config'], **config['consumer.txs.contract_interaction'])
  consumer_txs_sc_int.subscribe([topic_in_raw_txs])


  ethercan_api = EthercanAPI(api_key_scan, network)
  tx_converter = TransactionConverter(network, api_key_node, ethercan_api)
  MONITORED_ADDR = ['0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D']

  
  for tx_data in tx_converter.consuming_topic(consumer_txs_sc_int):
    contract_address = tx_data['to']
    input_data = tx_data['input']
    if contract_address in MONITORED_ADDR:
      input_data = tx_converter.decode_input(contract_address, input_data)
      print("MONITORED", input_data)
    else: 
      input_data = tx_converter.decode_input(contract_address, input_data)
      print("NON MONITORED", input_data)