from kafka import KafkaProducer
import config
import time

def on_success(record_meta):
    print(f"Topic: {record_meta.topic}\nPartition: {record_meta.partition}\nOffset: {record_meta.offset}\n")

def on_failure(excep):
    print(excep)

producer = KafkaProducer(
    bootstrap_servers=config.BOOTSTRAP_SERVERS,
    sasl_mechanism=config.SASL_MECHANISM,
    security_protocol=config.SECURITY_PROTOCOL,
    sasl_plain_username=config.SASL_PLAIN_USERNAME,
    sasl_plain_password=config.SASL_PLAIN_PASSWORD
    )
try:
    for i in range(10):
        message_key = f"id_{i}"
        message = f"Hello world {i}"
        record_metadata = producer.send(
            config.TOPIC_NAME, key=message_key.encode(), value=message.encode(),
            ).add_callback(on_success).add_errback(on_failure)
except Exception as e:
    print("Error producing a message", str(e))
finally:
    producer.flush()
    producer.close()

