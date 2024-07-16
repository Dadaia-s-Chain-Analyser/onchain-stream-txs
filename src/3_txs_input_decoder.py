import argparse
import json
import logging
import os


from functools import lru_cache
from json.decoder import JSONDecodeError
from web3 import Web3
from confluent_kafka import Producer, Consumer
from azure.keyvault.secrets import SecretClient
from dadaia_tools.etherscan_client import EthercanAPI
from azure.identity import DefaultAzureCredential
from configparser import ConfigParser

from utils.dm_utils import DataMasterUtils
from utils.blockchain_node_connector import BlockchainNodeConnector
from utils.dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler


class TransactionConverter(BlockchainNodeConnector):


  def __init__(self, logger, network, akv_client, api_key_node_name, ethercan_api):
    super().__init__(logger, akv_client, network)
    self.etherscan_api = ethercan_api
    self.web3 = super().get_node_connection(api_key_node_name, 'alchemy')


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
    
  APP_NAME = "TRANSACTION_INPUT_CONVERSOR"
  NETWORK = os.environ["NETWORK"]
  KAFKA_BROKERS = {'bootstrap.servers': os.getenv("KAFKA_BROKERS")}
  TOPIC_LOGS = os.getenv('TOPIC_LOGS')
  TOPIC_TX_CONTRACT_CALL = os.getenv('TOPIC_TX_CONTRACT_CALL')
  TOPIC_TX_CONTRACT_CALL_DECODED = os.getenv('TOPIC_TX_CONTRACT_CALL_DECODED')
  SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
  AKV_NODE_NAME = os.getenv('AKV_NODE_NAME')
  AKV_SCAN_NAME = os.getenv('AKV_SCAN_NAME')
  AKV_NODE_SECRET_NAME = os.getenv('AKV_NODE_SECRET_NAME')
  AKV_SCAN_SECRET_NAME = os.getenv('AKV_SCAN_SECRET_NAME')

  credential = DefaultAzureCredential()
  AKV_NODE_URL = f'https://{AKV_NODE_NAME}.vault.azure.net/'
  AKV_NODE_CLIENT = SecretClient(vault_url=AKV_NODE_URL, credential=DefaultAzureCredential())

  AKV_SCAN_URL = f'https://{AKV_SCAN_NAME}.vault.azure.net/'
  AKV_SCAN_CLIENT = SecretClient(vault_url=AKV_SCAN_URL, credential=DefaultAzureCredential())

  api_key_scan = AKV_SCAN_CLIENT.get_secret(AKV_SCAN_SECRET_NAME)

  parser = argparse.ArgumentParser(description=f'Stream cleaned transactions network')
  parser.add_argument('config_producer', type=argparse.FileType('r'), help='Config Producers')
  parser.add_argument('config_consumer', type=argparse.FileType('r'), help='Config Consumers')
  args = parser.parse_args()

  # Parse de dados provenientes dos arquivos de configuração para variável config
  config = ConfigParser()
  config.read_file(args.config_producer)
  config.read_file(args.config_consumer)
  
  # Criação de 1 consumer para o tópico de hash_txs e 1 producer para o tópico de raw_txs
  create_producer = lambda special_config: Producer(**KAFKA_BROKERS, **config['producer.general.config'], **config[special_config])
  PRODUCER_LOGS = create_producer('producer.logs.application')
  producer_parsed_input_txs = create_producer('producer.txs.input.decoded')
  CONSUMER_TX_CONTRACT_CALL = Consumer(**KAFKA_BROKERS, **config['consumer.txs.contract_interaction'])
  CONSUMER_TX_CONTRACT_CALL.subscribe([TOPIC_TX_CONTRACT_CALL])

  # Configurando Logging para console e Kafka
  LOGGER = logging.getLogger(APP_NAME)
  LOGGER.setLevel(logging.INFO)
  kafka_handler = KafkaLoggingHandler(PRODUCER_LOGS, TOPIC_LOGS)
  ConsoleLoggingHandler = ConsoleLoggingHandler()
  LOGGER.addHandler(ConsoleLoggingHandler)
  LOGGER.addHandler(kafka_handler)


  ethercan_api = EthercanAPI(api_key_scan, NETWORK)
  TRANSACTION_CONVERTER = TransactionConverter(LOGGER, NETWORK, AKV_NODE_CLIENT, AKV_NODE_SECRET_NAME, ethercan_api)

  MONITORED_ADDR = ['0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',
                    '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45',
                    '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9',
                    '0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2'
                    ]
  for tx_data in TRANSACTION_CONVERTER.consuming_topic(CONSUMER_TX_CONTRACT_CALL):
    contract_address = tx_data['to']
    input_data = tx_data['input']
    print(contract_address, input_data)
    if contract_address in MONITORED_ADDR:
      converted_input_data = TRANSACTION_CONVERTER.decode_input(contract_address, input_data)
      print("MONITORED", converted_input_data)
    else:
      continue