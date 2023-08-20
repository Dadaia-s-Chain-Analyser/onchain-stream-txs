from azure.eventhub.aio import EventHubProducerClient, EventHubConsumerClient
from azure.eventhub import EventData


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