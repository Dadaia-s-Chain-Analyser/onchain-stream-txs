import logging

class SuccessHandler:

  def __init__(self, logger):
    self.logger = logger

  def __call__(self, record_metadata):
    key = record_metadata.key().decode('utf-8') if record_metadata.key() else None
    partition = record_metadata.partition()
    topic = record_metadata.topic()
    self.logger.info(f"Kafka_Ingestion;TOPIC:{topic};PARTITION:{partition};KEY:{key}")
    

class ErrorHandler:

  def __init__(self, logger):
    self.logger = logger

  def __call__(self, error):
    self.logger.error(f"Error: {error}")