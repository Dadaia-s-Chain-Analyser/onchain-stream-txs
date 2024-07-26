from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka import DeserializingConsumer, SerializingProducer
from utils.kafka_handlers import SuccessHandler, ErrorHandler



class SchemaRegistryHandler:
  def __init__(self, logger, schema_registry_url):
    self.schema_registry_url = schema_registry_url
    self.logger = logger


  def create_avro_consumer(self, consumer_configs, avro_schema) -> DeserializingConsumer:
    schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})
    avro_deserializer = AvroDeserializer(schema_registry_client, avro_schema, lambda msg, ctx: dict(msg))
    consumer_configs['key.deserializer'] = StringDeserializer('utf_8')
    consumer_configs['value.deserializer'] = avro_deserializer
    return DeserializingConsumer(consumer_configs)


  def create_avro_producer(self, producer_configs, avro_schema) -> SerializingProducer:
    schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})
    avro_serializer = AvroSerializer(schema_registry_client, avro_schema, lambda msg, ctx: dict(msg))
    producer_configs['key.serializer'] = StringSerializer('utf_8')
    producer_configs['value.serializer'] = avro_serializer
    return SerializingProducer(producer_configs)
  

  def message_handler(self, err, msg):
    if err is not None: ErrorHandler(self.logger)(err)
    else: SuccessHandler(self.logger)(msg)