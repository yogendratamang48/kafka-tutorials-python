from kafka import KafkaConsumer
import config
import sys
import logging
import signal
import yaml
import logging.config

# Initialize the logger once as the application starts up.
with open("logging.yaml", 'rt') as f:
    config_log = yaml.safe_load(f.read())
    logging.config.dictConfig(config_log)

logger = logging.getLogger(__name__)

class Killer:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.shutdown_signal = False

    def exit_gracefully(self, signal_no, stack_frame):
        logger.info("Sigterm handler")
        self.shutdown_signal = True
        raise SystemExit

class Cons:
    def __init__(self):
        signal.signal(signal.SIGINT, self.sigterm_handler)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        self.consumer = KafkaConsumer(
                bootstrap_servers=config.BOOTSTRAP_SERVERS,
                sasl_mechanism=config.SASL_MECHANISM,
                security_protocol=config.SECURITY_PROTOCOL,
                sasl_plain_username=config.SASL_PLAIN_USERNAME,
                sasl_plain_password=config.SASL_PLAIN_PASSWORD,
                value_deserializer=lambda m: m.decode(),
                group_id = "my-python-app-2",
                enable_auto_commit=True
                )
        self.consumer.subscribe([config.TOPIC_NAME])

    def sigterm_handler(self, signum, frame):
        logger.info("Sigterm handler")
        self.consumer.close(autocommit=False)
        sys.exit(0)
        
    def consume(self):
            try:
                while True:
                    msg = self.consumer.poll(1000)
                    if msg:
                        for topic, records in msg.items():
                            for record in records:
                                logger.info(f'Topic: {record.topic}, Partion: {record.partition}, Offset: {record.partition}')
                                logger.info(f'Message: {record.value}, Key: {record.key}')
            except ValueError as e:
                logger.info('Shutting down...')

if __name__ == '__main__':
    c=Cons()
    c.consume()