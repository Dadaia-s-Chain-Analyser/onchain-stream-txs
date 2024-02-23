import argparse
import datetime as dt
import hexbytes
import json
import logging
import os
import time

from configparser import ConfigParser
from requests import HTTPError
from dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler
from dm_BNaaS_connector import BlockchainNodeAsAServiceConnector
from confluent_kafka import Producer
from dadaia_tools.azure_key_vault_client import KeyVaultAPI
from azure.identity import DefaultAzureCredential


class BlockMiner(BlockchainNodeAsAServiceConnector):

    def __init__(self, network, api_key_node, tx_threshold=None, frequency=1, kv_secret_name=None):
        self.tx_threshold = tx_threshold
        self.frequency = float(frequency)
        self.counter = 0
        self.kv_secret_name = kv_secret_name
        self.web3 = self.get_node_connection(network, api_key_node, 'alchemy')


    def __get_latest_block(self):
        try:
            block_info = self.web3.eth.get_block('latest')
            datetime = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"Time:{datetime};API Key:{self.kv_secret_name};Request Number:{self.counter}")
            return block_info
        except HTTPError as e:
            logging.error(f"Time:{datetime};API Key:{self.kv_secret_name};Request Number:{self.counter};Error:{str(e)}")
            if str(e)[:3] == '429': return
            return

    def __parse_block_data(self, block):
        block_data = {k: v for k, v in block.items() if k not in ('transactions', 'withdrawals')}
        transactions = [bytes.hex(i) for i in block['transactions']]
        block_data['transactions'] = transactions
        block_data = {k: bytes.hex(v) if type(v) == hexbytes.main.HexBytes else v for k, v in block_data.items()}
        withdrawals = [dict(i) for i in block['withdrawals']]
        block_data['withdrawals'] = withdrawals
        block_data['totalDifficulty'] = str(block_data['totalDifficulty'])
        return block_data

    def limit_transactions(self, block_data):
        """Limita o número de transações por bloco de acordo com o valor de tx_threshold
        Usado para limitar o número de requisições a API em testes
        """
        return block_data["transactions"] if self.tx_threshold == 0 \
                        else block_data["transactions"][:self.tx_threshold]
    

    def streaming_block_data(self):
        """Gera um stream de blocos minerados na rede EVM
        A cada frequência de clock, verifica se houve mineração de um novo bloco com uma requisição a API
        Gera dados do bloco em formato JSON se houve mineração de um novo bloco
        """
        previous_block = 0
        self.counter = 0
        while 1:
            actual_block = self.__get_latest_block()
            if not actual_block: continue
            if actual_block != previous_block:
                block_data = self.__parse_block_data(actual_block)
                yield block_data
                previous_block = actual_block
            time.sleep(float(self.frequency))


if __name__ == '__main__':
    
    # Leitura de variáveis de ambiente
    network = os.environ["NETWORK"]
    akv_node_name = os.environ['KEY_VAULT_NODE_NAME']
    akv_secret_name = os.environ['KEY_VAULT_NODE_SECRET']
    
    # Configuração de argumentos a serem passados via linha de comando e leitura do arquivo de configuração
    parser = argparse.ArgumentParser(description=f'Streaming de blocos minerados na rede EVM {network}')
    parser.add_argument('config_producer', type=argparse.FileType('r'), help='Configurações de producers')
    parser.add_argument('--frequency', type=float, help='Frequência de clock', default=1)
    parser.add_argument('--tx_threshold', type=int, help='Limite transações por bloco', default=8)
    args = parser.parse_args()
    config = ConfigParser()
    config.read_file(args.config_producer)

    # Obtendo API Key do Azure Key Vault
    credential = DefaultAzureCredential()
    key_vault_api = KeyVaultAPI(akv_node_name, credential)
    api_key_node = key_vault_api.get_secret(akv_secret_name)

    # Configurando producers relativos a: block_metadata, hash_txs e logs
    make_topic_name = lambda topic: f"{network}.{dict(config[topic])['topic']}"
    producer_block_metadata = Producer(**config['kafka.cluster'], **config['producer.block_metadata'])
    producer_hash_txs = Producer(**config['kafka.cluster'], **config['producer.hash_txs'])
    producer_logs = Producer(**config['kafka.cluster'], **config['producer.logs.block_clock'])

    # Obter nome dos tópicos a partir do arquivo de configuração
    topic_logs = make_topic_name('topic.logs.clock')
    topic_name_block_metadata = make_topic_name('topic.block_metadata')
    topic_name_block_hash_txs = make_topic_name('topic.hash_txs')
    topic_num_partitions_hash_txs = dict(config['topic.hash_txs'])['num_partitions']
   
    # Configurando Logging
    logger = logging.getLogger("")
    logger.setLevel(logging.INFO)
    kafka_handler = KafkaLoggingHandler(producer_logs, topic_logs)
    ConsoleLoggingHandler = ConsoleLoggingHandler()
    logger.addHandler(ConsoleLoggingHandler)
    logger.addHandler(kafka_handler)

    # Inicializa ingestão de blocos e hash_id de suas respectivas transações
    block_miner = BlockMiner(network, api_key_node, args.tx_threshold, args.frequency, akv_secret_name)
    for block_data in block_miner.streaming_block_data():
        encoded_message = json.dumps(block_data).encode('utf-8')
        producer_block_metadata.produce(topic_name_block_metadata, value=encoded_message)
        producer_block_metadata.flush()
        logger.info(f"tx_read;{len(block_data['transactions'])};{block_data['number']}")
        counter = 0
        transactions = block_miner.limit_transactions(block_data)
        for tx_data in transactions:
            partition = counter % int(topic_num_partitions_hash_txs)
            counter += 1
            counter = 0 if counter == int(topic_num_partitions_hash_txs) else counter
            producer_hash_txs.produce(topic=topic_name_block_hash_txs, value=tx_data, partition=partition)
        producer_hash_txs.flush()
        logger.info(f"tx_sent;{len(transactions)};{block_data['number']}")

    # TODO
    # 3. Configurar producer de hash_txs com microbatching
    # 4. Ver se faz sentido usar keys para os tópicos
        
         

    # mylogger = logging.getLogger()
    # mylogger.addHandler(logging.StreamHandler())
    # producer = confluent_kafka.Producer({'bootstrap.servers': 'mybroker.com'}, logger=mylogger)

