from azure.identity import DefaultAzureCredential
from azure.cosmos import CosmosClient


class CosmosCoreAPI:

    def __init__(self, url, credential):
        self.client = CosmosClient(url, credential)
    
    def get_database_client(self, database_name):
        return self.client.get_database_client(database_name)
    
    def get_databases(self):
        return list(self.client.list_databases())
    
    def get_containers(self, database_name):
        return list(self.get_database_client(database_name).list_containers())
    
    def get_container_client(self, database_name, container_name):
        return self.get_database_client(database_name).get_container_client(container_name)
    
    def get_item_client(self, database_name, container_name, item_id):
        return self.get_container_client(database_name, container_name).read_item(item_id)
    
    def query_items(self, database_name, container_name, query):
        return self.get_container_client(database_name, container_name).query_items(query, enable_cross_partition_query=True)


    def create_database(self, database_name, prov_throughput=1000):
        self.client.create_database(
            database_name, 
            offer_throughput=prov_throughput
        )

if __name__ == '__main__':
    
    cosmodb_url = 'https://dadaiacosmodb.documents.azure.com:443'
    cosmos_api = CosmosCoreAPI(cosmodb_url, credential=DefaultAzureCredential())

    databases = cosmos_api.get_databases()
    print("INFORMATION ABOUT DATABASES")
    print(databases)
    
    #containers = cosmos_api.get_containers(databases[0]['id'])
    #print(f"INFORMATION ABOUT CONTAINERS INSIDE {databases[0]['id']}")
    #print(containers)

    print("CREATING DATABASE")
    cosmos_api.create_database('testdatabase')




