from kafka import KafkaConsumer
import config
import time
import logging
import yaml
import logging.config

# Initialize the logger once as the application starts up.
with open("logging.yaml", 'rt') as f:
    config_log = yaml.safe_load(f.read())
    logging.config.dictConfig(config_log)

logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    bootstrap_servers=config.BOOTSTRAP_SERVERS,
    sasl_mechanism=config.SASL_MECHANISM,
    security_protocol=config.SECURITY_PROTOCOL,
    sasl_plain_username=config.SASL_PLAIN_USERNAME,
    sasl_plain_password=config.SASL_PLAIN_PASSWORD,
    value_deserializer=lambda m: m.decode(),
    group_id = "my-python-app-2",
    enable_auto_commit=True
    )

consumer.subscribe([config.TOPIC_NAME])
while True:
    msg = consumer.poll(1000)
    if msg:
        for topic, records in msg.items():
            for record in records:
                logger.info(f'Topic: {record.topic}, Partion: {record.partition}, Offset: {record.partition}')
                logger.info(f'Message: {record.value}, Key: {record.key}')
