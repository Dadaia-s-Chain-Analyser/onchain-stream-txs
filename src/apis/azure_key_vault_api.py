import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv


class KeyVaultAPI:


    def __init__(self, key_vault_name, credential):
            key_vault_url = f"https://{key_vault_name}.vault.azure.net/"
            self.client = SecretClient(vault_url=key_vault_url, credential=credential)


    def get_secret(self, secret_name):
        secret = self.client.get_secret(secret_name)
        return secret.value

if __name__ == "__main__":
    
    load_dotenv()

    KEY_VAULT_SCAN_NAME = os.environ['KEY_VAULT_SCAN_NAME']
    
    credential = DefaultAzureCredential()
    key_vault_api = KeyVaultAPI(KEY_VAULT_SCAN_NAME, credential)



    # res1 = key_vault_api.get_secret("etherscan-api-key-1")
    # res2 = key_vault_api.get_secret("etherscan-api-key-2")

    #print(res1, res2)
    for i in range(1,6):
         res = key_vault_api.get_secret(f"etherscan-api-key-{i}")
         print(res)