import requests, logging
from requests.exceptions import InvalidSchema, ConnectionError


class EthercanAPI:

    def __init__(self, api_key, network):
        DICT_NETWORK = {'mainnet': 'api.etherscan.io','goerli': 'api-goerli.etherscan.io', 'polygon-main': 'polygonscan.com'}
        self.api_key = api_key
        self.url = f"https://{DICT_NETWORK[network]}/api"


    def get_eth_balance_url(self, address):
        base_uri_method = "module=account&action=balance"
        return f"{self.url}?{base_uri_method}&address={address}&tag=latest&apikey={self.api_key}"
    

    def get_block_by_time_url(self, timestamp, closest='before'):
        base_uri_method = "module=block&action=getblocknobytime"
        return f"{self.url}?{base_uri_method}&timestamp={timestamp}&closest={closest}&apikey={self.api_key}"


    def get_logs_url(self, address, fromblock, toblock, page=1, offset=100):
        base_uri_method = "module=logs&action=getLogs"
        return f"{self.url}?{base_uri_method}&address={address}&" + \
            f"fromBlock={fromblock}&toBlock={toblock}&page={page}&offset={offset}&apikey={self.api_key}"


    def get_txlist_url(self, address, startblock, endblock, page=1, offset=100, sort='asc'):
        base_uri_method = "module=account&action=txlist"
        return f"{self.url}?{base_uri_method}&address={address}" + \
            f"&startblock={startblock}&endblock={endblock}&page={page}" + \
            f"&offset={offset}&sort={sort}&apikey={self.api_key}"


    def get_abi_url(self, address):
        base_uri_method = "module=contract&action=getabi"
        return f"{self.url}?{base_uri_method}&address={address}&apikey={self.api_key}"


    def req_chain_scan(self, request_url):
        try: response = requests.get(request_url)
        except InvalidSchema as e: logging.error(e) ; return False
        except ConnectionError as e: logging.error(e) ; return False
        else:
            if response.status_code == 200:
                content = response.json()
                if content['status'] == '1':
                    return content['result']
                return False


if __name__ == '__main__':
    pass