import os
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

class TopicCreator:

    def __init__(self, network, config, cluster_info):
        self.network = network
        self.config = config
        self.cluster_info = cluster_info


    def __create_topic(self, 
                       name, 
                       num_partitions, 
                       replication_factor, 
                       topic_configs={}, 
                       overwrite=False,
                       use_network=True
        ):
        kafka_admin = KafkaAdminClient(bootstrap_servers=self.cluster_info['bootstrap_servers'])
        name = f"{self.network}.{name}" if use_network else name
        topic = NewTopic(
            name=name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=topic_configs)
        try:
            kafka_admin.create_topics(new_topics=[topic], validate_only=False)
            print(f'Tópico {name} criado')
        except TopicAlreadyExistsError:
            print(f'Tópico {name} já existe')
            if overwrite:
                time.sleep(1)
                print(f'Deletando tópico {name}')
                kafka_admin.delete_topics([name])
                time.sleep(5)
                print(f'Criando tópico {name}')
                kafka_admin.create_topics(new_topics=[topic], validate_only=False)
                print(f'Novo tópico {name} criado com sucesso!\n')


    def __format_int_parms(self, dic, int_parms=['num_partitions', 'replication_factor']):
        return {k: int(v) if k in int_parms else v for k, v in dic.items()}


    def make_topic_from_configs(self, topic, special_config={}, overwrite=False, use_network=True):
        topic_parms = dict(self.config[topic])
        topic_parms = self.__format_int_parms(topic_parms)
        topic_configs = {**self.config["topic.general.config"], **special_config}
        topic_conf = {**topic_parms, 'topic_configs': topic_configs}
        self.__create_topic(**topic_conf, overwrite=overwrite, use_network=use_network)

if __name__ == "__main__":

    network = os.environ["NETWORK"]
    parser = ArgumentParser(description=f'Stream transactions network')
    parser.add_argument('config_file', type=FileType('r'), help='Config file')
    args = parser.parse_args()
    config = ConfigParser()
    config.read_file(args.config_file)
    cluster_info = dict(config['kafka.cluster'])

    topics_maker = TopicCreator(network, config, cluster_info)
    topics_maker.make_topic_from_configs('topic.block_metadata')
    topics_maker.make_topic_from_configs('topic.hash_txs')
    topics_maker.make_topic_from_configs('topic.raw_txs')
    topics_maker.make_topic_from_configs('topic.txs.native_token_transfer')
    topics_maker.make_topic_from_configs('topic.txs.contract_interaction')
    topics_maker.make_topic_from_configs('topic.txs.contract_deployment')
    topics_maker.make_topic_from_configs('topic.application.logs', use_network=False)

    special_config = config['topic.shared.api_keys.config']
    topics_maker.make_topic_from_configs('topic.shared.api_keys', special_config=special_config, use_network=False)

