import json, asyncio
from abc import ABC, abstractmethod
from azure.eventhub.aio import EventHubProducerClient, EventHubConsumerClient
from azure.eventhub import EventData
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

    

class KafkaStreamer(metaclass=SingletonMeta):

  
    def __init__(self, connection_str):
        self.connection_str = connection_str


    def create_producer(self):
        partitioner = lambda key, all, available: 0
        json_serializer = lambda data: json.dumps(data).encode('utf-8')
        self.producer = KafkaProducer(
                                        bootstrap_servers=self.connection_str,
                                        value_serializer=json_serializer, 
                                        partitioner=partitioner
        )
   

    def create_consumer(self, topic, group_id):
        self.consumer = KafkaConsumer(
                                        topic, 
                                        bootstrap_servers=self.connection_str, 
                                        auto_offset_reset='latest', 
                                        group_id=group_id
        )


    def create_kafka_topic_idempotent(self, topic, num_partitions, replication_factor, overwrite=False):
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

    def send_data(self, topic, data, partition=None, key="0"):
        self.producer.send(topic=topic, key=f"topic_{key}".encode('utf-8'), partition=partition, value=data)
        self.producer.flush()


class EventHubStreamer(Streamer):

    def __init__(self, connection_str, ):
        self.connection_str = connection_str


    def create_producer(self, eventhub_name):
        self.producer = EventHubProducerClient.from_connection_string(
                                        conn_str=self.connection_str, 
                                        eventhub_name=eventhub_name
        )

    def create_consumer(self, eventhub_name, consumer_group):
        self.consumer = EventHubConsumerClient.from_connection_string(
                                        conn_str=self.connection_str, 
                                        consumer_group=consumer_group, 
                                        eventhub_name=eventhub_name
        )


    def send_data(self, data):
        async def run():
            async with self.producer:
                event_data_batch = await self.producer.create_batch()
                event_data_batch.add(EventData(json.dumps(data)))
                await self.producer.send_batch(event_data_batch)
        asyncio.run(run())



class ChainStreamer:

    def config_producer_kafka(self, connection_str, topic, num_partitions=1):
        self.topic_produce = topic
        self.num_partitions = num_partitions
        self.kafka_streamer = KafkaStreamer(connection_str)
        self.kafka_streamer.create_producer()
        self.kafka_streamer.create_kafka_topic_idempotent(topic, num_partitions, 1, overwrite=False)

    def config_consumer_kafka(self, connection_str, topic, consumer_group):
        self.kafka_streamer = KafkaStreamer(connection_str)
        self.kafka_streamer.create_consumer(topic, consumer_group)

    def config_producer_event_hub(self, connection_str, eventhub_name):
        self.event_hub_name = eventhub_name
        self.event_hub_streamer = EventHubStreamer(connection_str)
        self.event_hub_streamer.create_producer(eventhub_name)

    def _produce_data(self, data):
        if self.to_cloud: 
            print("tem que cair aqui")
            self.event_hub_streamer.send_data(data)
        else: 
            print("MAS CAI AQUI PORRA")
            self.kafka_streamer.send_data(self.event_hub_name, data)
        
