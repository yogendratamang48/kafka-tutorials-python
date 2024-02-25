from kafka import KafkaConsumer
import config
import time
import logging
import signal

logger = log = logging.getLogger(__name__)

class Killer:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.shutdown_signal = False

    def exit_gracefully(self, signal_no, stack_frame):
        self.shutdown_signal = True
        raise SystemExit

class Cons:
    def __init__(self):
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
        
    def consume(self):
        
        killer = Killer()
        while not killer.shutdown_signal:
            try:
                self.consumer.subscribe([config.TOPIC_NAME])
                msg = self.consumer.poll(1000)
                if msg:
                    for topic, records in msg.items():
                        for record in records:
                            logger.info(f'Topic: {record.topic}, Partion: {record.partition}, Offset: {record.partition}')
                            logger.info(f'Message: {record.value}, Key: {record.key}')
            except SystemExit:
                logging.info('Shutting down...')
            finally:
                self.consumer.close()

if __name__ == '__main__':
    c=Cons()
    c.consume()