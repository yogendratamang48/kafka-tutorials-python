from kafka import KafkaProducer
import config

producer = KafkaProducer(
    bootstrap_servers=config.BOOTSTRAP_SERVERS,
    sasl_mechanism=config.SASL_MECHANISM,
    security_protocol=config.SECURITY_PROTOCOL,
    sasl_plain_username=config.SASL_PLAIN_USERNAME,
    sasl_plain_password=config.SASL_PLAIN_PASSWORD
)

try:
    producer.send(config.TOPIC_NAME, b'Hello from python')
    producer.flush()
    print("Message produced without Avro schema!")
except Exception as e:
    print(f"Error producing message: {e}")
finally:
    producer.close()