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
        return Web3(Web3.IPCProvider("/root/.ethereum/geth.ipc"))
    
if __name__ == "__main__":
    bnaas = BlockchainNodeAsAServiceConnector()
    web3 = bnaas.get_node_connection("mainnet", "api-key", "infura")
    print(web3.eth.get_block('latest'))