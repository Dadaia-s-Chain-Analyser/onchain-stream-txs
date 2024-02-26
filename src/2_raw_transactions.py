import argparse
import hexbytes
import json
import logging
import os
from hashlib import sha256

from azure.identity import DefaultAzureCredential
from dadaia_tools.azure_key_vault_client import KeyVaultAPI
from confluent_kafka import Producer, Consumer
from confluent_kafka.error import KafkaError, KafkaException
from configparser import ConfigParser

from dm_utils import DataMasterUtils
from dm_BNaaS_connector import BlockchainNodeAsAServiceConnector
from dm_logger import ConsoleLoggingHandler, KafkaLoggingHandler


class RawTransactionsProcessor(BlockchainNodeAsAServiceConnector):

    def __init__(self, network, api_key_node, vendor, logger):
        self.network = network
        self.api_key_node = api_key_node
        self.web3 = self._get_node_connection(network, api_key_node, vendor)
        self.utils = DataMasterUtils()
        self.logger = logger

    def consuming_topic(self, consumer):
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg: yield msg.value().decode('utf-8')

    def process_msg(self, tx_id):
        tx_data = self.web3.eth.get_transaction(tx_id)
        tx_data = {k: bytes.hex(v) if type(v) == hexbytes.main.HexBytes else v for k, v in tx_data.items()}
        if tx_data.get("accessList"):
            tx_data["accessList"] = [dict(i) for i in tx_data["accessList"]]
            tx_data["accessList"] = [self.utils.convert_hex_to_hexbytes(tx_data) for tx_data in tx_data["accessList"]]
        return tx_data
        

if __name__ == '__main__':

    network = os.environ["NETWORK"]
    key_vault_node_name = os.environ['KEY_VAULT_NODE_NAME']
    key_vault_node_secret = os.environ['KEY_VAULT_NODE_SECRET']
    parser = argparse.ArgumentParser(description=f'Stream transactions network')
    parser.add_argument('config_producer', type=argparse.FileType('r'), help='Config Producers')
    parser.add_argument('config_consumer', type=argparse.FileType('r'), help='Config Consumers')
    args = parser.parse_args()

    # Parse de dados provenientes dos arquivos de configuração para variável config
    config = ConfigParser()
    config.read_file(args.config_producer)
    config.read_file(args.config_consumer)

    # Nome de tópicos provenientes do arquivo de configuração
    make_topic_name = lambda topic: f"{network}.{dict(config[topic])['topic']}"
    topic_out_logs = make_topic_name('topic.app.logs')
    topic_in_hash_id_txs = make_topic_name('topic.hash_txs')
    topic_out_raw_txs = make_topic_name('topic.raw_txs')

    # Criação de 1 consumer para o tópico de hash_txs e 1 producer para o tópico de raw_txs
    create_producer = lambda special_config: Producer(**config['producer.general.config'], **config[special_config])
    producer_txs_raw = create_producer('producer.raw_txs')
    producer_logs = create_producer('producer.logs.block_clock')
    consumer_txs_hash = Consumer(**config['consumer.general.config'], **config['consumer.hash_txs'])
    consumer_txs_hash.subscribe([topic_in_hash_id_txs])

    # Configurando Logging para console e Kafka
    logger = logging.getLogger("raw-transactions")
    logger.setLevel(logging.INFO)
    kafka_handler = KafkaLoggingHandler(producer_logs, topic_out_logs)
    ConsoleLoggingHandler = ConsoleLoggingHandler()
    logger.addHandler(ConsoleLoggingHandler)
    logger.addHandler(kafka_handler)

    # Usa Azure Key vault para pegar as API KEYS guardadas lá como secrets.
    credential = DefaultAzureCredential()
    key_vault_api = KeyVaultAPI(key_vault_node_name, credential)
    interval_keys = [int(i) for i in key_vault_node_secret.split("-")[-2:]]
    name_secret = "-".join(key_vault_node_secret.split("-")[:-2])
    vendor = key_vault_node_secret.split("-")[0]
    api_keys = [key_vault_api.get_secret(f"{name_secret}-{i}") for i in range(interval_keys[0], interval_keys[1] + 1)]
    api_key = api_keys[0]

    # Inicializa processo de streaming com Source = hash_id_txs -> process -> Sink = raw_txs
    raw_tx_processor = RawTransactionsProcessor(network, api_key, vendor, logger)
    try:
        for msg in raw_tx_processor.consuming_topic(consumer_txs_hash):
            tx_data = raw_tx_processor.process_msg(msg)
            logger.info(f"tx_processed;{tx_data['hash']};{tx_data['blockNumber']}")
            encoded_message = json.dumps(tx_data).encode('utf-8')
            producer_txs_raw.produce(topic_out_raw_txs, value=encoded_message)
            producer_txs_raw.flush()
    finally: consumer_txs_hash.close()