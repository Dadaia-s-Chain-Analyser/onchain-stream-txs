from web3 import Web3

class BlockchainNodeConnector:

    def __init__(self, logger, akv_client, network):
        self.akv_client = akv_client
        self.logger = logger
        self.network = network

    def get_node_connection(self, api_key_name, vendor):
        api_key = self.akv_client.get_secret(api_key_name).value
        self.logger.info(f"KEY_VAULT_REQUEST: {api_key_name}")
        dict_vendors = { 
        'alchemy': f"https://eth-{self.network}.g.alchemy.com/v2/{api_key}",
        'infura': f"https://{self.network}.infura.io/v3/{api_key}"
        }
        vendor_url = dict_vendors.get(vendor)
        web3 = Web3(Web3.HTTPProvider(vendor_url))
        self.logger.info(f"API_KEY set;{api_key_name}")
        return web3