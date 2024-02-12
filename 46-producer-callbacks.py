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
    sasl_plain_password=config.SASL_PLAIN_PASSWORD,
    batch_size=400,
    
    )
try:
    for j in range(30):
        for i in range(10):
            record_metadata = producer.send(
                config.TOPIC_NAME, b'Hello World'
                ).add_callback(on_success).add_errback(on_failure)
        time.sleep(0.900)
    producer.flush()
except Exception as e:
    print("Error producing a message")
finally:
    
    producer.close()

