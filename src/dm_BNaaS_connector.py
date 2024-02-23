from web3 import Web3

class BlockchainNodeAsAServiceConnector:

    def __init__(self):
        pass

    def get_node_connection(self, network, api_key_node, vendor):
        dict_vendors = { 
        'alchemy': f"https://eth-{network}.g.alchemy.com/v2/{api_key_node}",
        'infura': f"https://{network}.infura.io/v3/{api_key_node}"
        }
        vendor_url = dict_vendors.get(vendor)
        Web3(Web3.HTTPProvider(vendor_url))
        return dict_vendors.get(vendor)