class MotherClass:


    def _get_url_node(self, network, api_key_node, vendor="infura"):
        dict_vendors = { 
        'alchemy': f"https://eth-{network}.g.alchemy.com/v2/{api_key_node}",
        'infura': f"https://{network}.infura.io/v3/{api_key_node}"
        }
        return dict_vendors.get(vendor)