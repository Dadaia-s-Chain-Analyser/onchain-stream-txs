import json, asyncio
from abc import ABC, abstractmethod
from kafka import KafkaProducer, KafkaConsumer
import os, sys, time
from kafka.admin import NewTopic, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
    


class Streamer(ABC):

    @abstractmethod
    def create_producer(self):
        raise NotImplementedError


    @abstractmethod
    def create_consumer(self):
        raise NotImplementedError


    @abstractmethod
    def send_data(self, data):
        raise NotImplementedError

    

class KafkaClient(metaclass=SingletonMeta):

  
    def __init__(self, connection_str):
        self.connection_str = connection_str


    def create_producer(self):
        partitioner = lambda key, all, available: 0
        json_serializer = lambda data: json.dumps(data).encode('utf-8')
        producer = KafkaProducer(
                                        bootstrap_servers=self.connection_str,
                                        value_serializer=json_serializer, 
                                        partitioner=partitioner
        )
        return producer
   

    def create_consumer(self, topic, consumer_group):
        consumer = KafkaConsumer(
                                        topic, 
                                        bootstrap_servers=self.connection_str, 
                                        auto_offset_reset='latest', 
                                        group_id=consumer_group
        )
        return consumer


    def create_idempotent_topic(self, topic, num_partitions=1, replication_factor=1, overwrite=False):
        admin = KafkaAdminClient(bootstrap_servers=self.connection_str)
        topic_blocks = NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)

        try: admin.create_topics(new_topics=[topic_blocks], validate_only=False)
        except TopicAlreadyExistsError: 
            if overwrite:
                admin.delete_topics([topic])
                time.sleep(5)
                admin.create_topics(new_topics=[topic_blocks], validate_only=False)
                return "TOPIC DELETED AND CREATED AGAIN"
            return "TOPIC ALREADY CREATED AND KEPT"
        else: return "TOPIC CREATED"

    def send_data(self, producer, topic, data, partition=None, key="0"):
        producer.send(topic=topic, key=f"topic_{key}".encode('utf-8'), partition=partition, value=data)
        producer.flush()




