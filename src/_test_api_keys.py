import logging
import time
from azure.identity import DefaultAzureCredential
from dadaia_tools.azure_key_vault_client import KeyVaultAPI
from dm_BNaaS_connector import BlockchainNodeAsAServiceConnector


class APIKeysAnalyser(BlockchainNodeAsAServiceConnector):

  def __init__(self, logger, network, akv_actor, num_reqs=50):
    self.__akv_actor = akv_actor
    self.__network = network
    self.__num_reqs = num_reqs
    self.__logger = logger

  def build_map(self, vendor, num_keys):
    apis_keys = [f"{vendor}-api-key-{i}" for i in range(1,num_keys + 1)]
    api_keys_map = {api_key: self.__akv_actor.get_secret(api_key) for api_key in apis_keys}
    return api_keys_map
    
  def get_api_key_performance(self, vendor, k_name, k_value):
    web3_obj = self.get_node_connection(network=self.__network, api_key_node=k_value, vendor=vendor)
    start_time = time.time()
    for req_count in range(self.__num_reqs):
      data = web3_obj.eth.get_block('latest')
      if req_count % 10 == 0:
          print(f"run {req_count} - {data['number']}")
    elapsed_time = time.time() - start_time
    self.__logger.info(f"api_key={k_name};num_reqs={self.__num_reqs};time_elapsed={elapsed_time}")
    return

  def run_test(self, vendor, num_keys):
    api_keys_map = test.build_map(vendor, num_keys)
    for k_name, k_value in api_keys_map.items():
      print(f"Testing {k_name}... vendor")
      self.get_api_key_performance(vendor, k_name, k_value)
    return

if __name__ == "__main__":

    VENDOR_INFURA = "infura"
    VENDOR_ALCHEMY = "alchemy"
    AKV_NAME = "DataMasterNodeAsAService"
    NETWORK = "mainnet"
    OUT_LOG_FILEPATH = "_logs/test_api_keys.log"

    # Obtendo API Key do Azure Key Vault
    credential = DefaultAzureCredential()
    key_vault_actor = KeyVaultAPI(AKV_NAME, credential)

    # Adding file logger with built in loggin
      # Configurando Logging
    logger = logging.getLogger("test-api-keys")
    logger.setLevel(logging.INFO)
    format = logging.Formatter('%(asctime)s;%(name)s;%(message)s')
    file_handler = logging.FileHandler(OUT_LOG_FILEPATH)
    file_handler.setFormatter(format)
    logger.addHandler(file_handler)
    
    # Testando performance das API Keys
    test = APIKeysAnalyser(logger, NETWORK, key_vault_actor, num_reqs=100)
    test.run_test(VENDOR_ALCHEMY, 5)
    test.run_test(VENDOR_INFURA, 17)


    






        