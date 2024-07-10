import logging
from confluent_kafka import Producer

    
class ConsoleLoggingHandler(logging.Handler):
    
  def __init__(self):
    super().__init__()
    self.formatter = logging.Formatter('%(asctime)s;%(name)s;%(levelname)s;%(message)s')

  def emit(self, record):
    print(self.format(record))

class FileLoggingHandler(logging.FileHandler):
      
    def __init__(self, filename):
      super().__init__(filename)
      self.formatter = logging.Formatter('%(asctime)s;%(name)s;%(levelname)s;%(message)s')

    def emit(self, record):
      super().emit(record)

class KafkaLoggingHandler(logging.Handler):

    def __init__(self, producer, topic):
        super().__init__()
        self.producer = producer
        self.topic = topic

    def emit(self, record):
        msg = self.format(record)
        self.producer.produce(self.topic, value=msg)
        self.producer.flush()