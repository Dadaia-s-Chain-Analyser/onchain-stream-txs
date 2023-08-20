import os, uuid
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError

load_dotenv()


class BlobApi:

    def __init__(self, conn_string):
        self.blob_service_client = BlobServiceClient.from_connection_string(conn_string)


    def create_container(self, container_name):
        try: container_client = self.blob_service_client.create_container(container_name)
        except ResourceExistsError: 
            print(f'Container {container_name} already exists')
            container_client = self.blob_service_client.get_container_client(container_name)
        return container_client


    def delete_container(self, container_name):
        container_client = self.blob_service_client.get_container_client(container_name)
        container_client.delete_container()
        return container_client


    def upload_blob(self, container_name, blob_name, file_path):  
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        try: 
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data)
        except ResourceExistsError: print(f'Blob {blob_name} already exists')
        return blob_client
    

    def download_blob(self, container_name, blob_name, file_path):
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        return blob_client
    

    def delete_blob(self, container_name, blob_name):
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.delete_blob()
        return blob_client
    
    
    def list_blobs(self, container_name):
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs()
        return blob_list
    

    def list_containers(self):
        container_list = self.blob_service_client.list_containers()
        return container_list
    

if __name__ == '__main__':

    pass
    # container_name = 'blob-api-test'
    # blob_name = 'people.csv'
    # path_file_input = f'../files/azure_inbound/{blob_name}'
    # path_file_output = f'../files/azure_outbound/{blob_name}'
    # conn_str = os.getenv('CONN_STRING_1')
    # blob_api = BlobApi(conn_str)


    # lista_containers = lambda blob_api: print(f"Containers: {[container.name for container in blob_api.list_containers()]}")
    # lista_blobs = lambda blob_api, container_name: print(f"Blobs inside {container_name}: {[blob.name for blob in blob_api.list_blobs(container_name)]}")

    # # lista os containers
    # lista_containers(blob_api)

    # # cria um container
    # container_client = blob_api.create_container(container_name)

    # # lista os containers

    # lista_containers(blob_api)

    # # lista os blobs do container

    # lista_blobs(blob_api, container_name)

    # # upload de um blob

    # blob_client = blob_api.upload_blob(container_name, "people.csv", path_file_input)

    # # lista os blobs

    # lista_blobs(blob_api, container_name)

    # # download de um blob

    # blob_client = blob_api.download_blob(container_name, "people.csv", path_file_output)

    # # deleta um blob

    # blob_client = blob_api.delete_blob(container_name, "people.csv")

    # # lista os blobs

    # lista_blobs(blob_api, container_name)

    # # deleta um container

    # container_client = blob_api.delete_container(container_name)

    # # lista os containers
    # lista_containers(blob_api)
