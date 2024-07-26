import argparse
import json
import logging
import os

from functools import lru_cache
from json.decoder import JSONDecodeError
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from configparser import ConfigParser

from dadaia_tools.etherscan_client import EthercanAPI
from utils.dm_utils import DataMasterUtils
from utils.blockchain_node_connector import BlockchainNodeConnector
from utils.dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler
from utils.schema_registry_handler import SchemaRegistryHandler

from confluent_kafka import Producer


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
    except Exception as e: print(e)
    else:
      for key in parms_method:
        if isinstance(parms_method[key], (bytes)): parms_method[key] = parms_method[key].hex()
        elif isinstance(parms_method[key], (list)): parms_method[key] = [x.hex() if isinstance(x, (bytes)) else x for x in parms_method[key]]
        elif isinstance(parms_method[key], (dict)): parms_method[key] = {k.hex() if isinstance(k, (bytes)) else k: v.hex() if isinstance(v, (bytes)) else v for k, v in parms_method[key].items()}
        method = str(method).split('<Function ')[1].split('>')[0]
        input_data = dict(method=method, parms=parms_method)
        return input_data


  def get_transaction_avro_schema(self):
    with open('schemas/transactions_schema_avro.json') as f:
      return json.dumps(json.load(f))
    

  def get_input_decoded_avro_schema(self):
    with open('schemas/txs_contract_call_decoded.json') as f:
      return json.dumps(json.load(f))
    

  def consuming_topic(self, consumer):
    while True:
      msg = consumer.poll(timeout=0.1)
      if msg: yield msg.value()


  def transform_input(self, tx_data, converted_input_data):
    if converted_input_data:
      if converted_input_data["method"] == 'multicall(uint256,bytes[])':
        converted_input_data["parms"]["data"] = [x.hex() for x in converted_input_data["parms"]["data"]]
      try:
        data = {
          "tx_hash": tx_data['hash'],
          "block_number": tx_data['blockNumber'],
          "from": tx_data['from'],
          "contract_address": contract_address,
          "input": tx_data['input'],
          "method": converted_input_data["method"],
          "parms": json.dumps(converted_input_data["parms"])
        }
      except Exception as e:
        self.logger.error(f"Error: {e}")
        self.logger.error(f"Data: {converted_input_data}")
        return None
      else: return data



if __name__ == '__main__':
    
  APP_NAME = "TRANSACTION_INPUT_CONVERSOR"
  NETWORK = os.environ["NETWORK"]
  KAFKA_BROKERS = {'bootstrap.servers': os.getenv("KAFKA_BROKERS")}
  TOPIC_LOGS = os.getenv('TOPIC_LOGS')
  GROUP_ID = os.getenv('KAFKA_CG_INPUT_DECODER')
  SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
  TOPIC_TX_CONTRACT_CALL = os.getenv('TOPIC_TX_CONTRACT_CALL')
  TOPIC_TX_CONTRACT_CALL_DECODED = os.getenv('TOPIC_TX_CONTRACT_CALL_DECODED')
  SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
  AKV_NODE_NAME = os.getenv('AKV_NODE_NAME')
  AKV_SCAN_NAME = os.getenv('AKV_SCAN_NAME')
  AKV_NODE_SECRET_NAME = os.getenv('AKV_NODE_SECRET_NAME')
  AKV_SCAN_SECRET_NAME = os.getenv('AKV_SCAN_SECRET_NAME')

  AKV_NODE_URL = f'https://{AKV_NODE_NAME}.vault.azure.net/'
  AKV_NODE_CLIENT = SecretClient(vault_url=AKV_NODE_URL, credential=DefaultAzureCredential())

  AKV_SCAN_URL = f'https://{AKV_SCAN_NAME}.vault.azure.net/'
  AKV_SCAN_CLIENT = SecretClient(vault_url=AKV_SCAN_URL, credential=DefaultAzureCredential())

  api_key_scan = AKV_SCAN_CLIENT.get_secret(AKV_SCAN_SECRET_NAME).value

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
  PRODUCER_LOGS = create_producer('producer.config.p1')
  producer_parsed_input_txs = create_producer('producer.config.p1')

  # Configurando Logging para console e Kafka
  LOGGER = logging.getLogger(APP_NAME)
  LOGGER.setLevel(logging.INFO)
  kafka_handler = KafkaLoggingHandler(PRODUCER_LOGS, TOPIC_LOGS)
  ConsoleLoggingHandler = ConsoleLoggingHandler()
  LOGGER.addHandler(ConsoleLoggingHandler)
  LOGGER.addHandler(kafka_handler)


  ethercan_api = EthercanAPI(api_key_scan, NETWORK)
  TRANSACTION_CONVERTER = TransactionConverter(LOGGER, NETWORK, AKV_NODE_CLIENT, AKV_NODE_SECRET_NAME, ethercan_api)
  SCHEMA_REGISTRY_HANDLER = SchemaRegistryHandler(LOGGER, SCHEMA_REGISTRY_URL)

  producer_avro_configs = {**KAFKA_BROKERS, **config['producer.general.config']}
  PRODUCER_TX_CONTRACT_CALL_DECODED = SCHEMA_REGISTRY_HANDLER.create_avro_producer(producer_avro_configs, TRANSACTION_CONVERTER.get_input_decoded_avro_schema())
  
  consumer_avro_configs = {**KAFKA_BROKERS, **config['consumer.general.config'], 'group.id': GROUP_ID}
  CONSUMER_TX_CONTRACT_CALL = SCHEMA_REGISTRY_HANDLER.create_avro_consumer(consumer_avro_configs, TRANSACTION_CONVERTER.get_transaction_avro_schema())
  CONSUMER_TX_CONTRACT_CALL.subscribe([TOPIC_TX_CONTRACT_CALL])

  MONITORED_ADDR = [
    '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',
    '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45',
    '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9',
    '0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2'
  ]

  for tx_data in TRANSACTION_CONVERTER.consuming_topic(CONSUMER_TX_CONTRACT_CALL):
    contract_address = tx_data['to']
    input_data = tx_data['input']
    if contract_address in MONITORED_ADDR:
      converted_input_data = TRANSACTION_CONVERTER.decode_input(contract_address, input_data)
      data_tx_decoded = TRANSACTION_CONVERTER.transform_input(tx_data, converted_input_data)
      if not data_tx_decoded:
        LOGGER.error(f"Error: {data_tx_decoded}")
        continue
      PRODUCER_TX_CONTRACT_CALL_DECODED.produce(
        topic=TOPIC_TX_CONTRACT_CALL_DECODED,
        value=data_tx_decoded,
        on_delivery=SCHEMA_REGISTRY_HANDLER.message_handler
      )

      PRODUCER_TX_CONTRACT_CALL_DECODED.flush()
    else:
      continue