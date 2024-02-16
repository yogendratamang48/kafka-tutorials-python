from kafka import KafkaConsumer
import config
import time
import logging
logging.getLogger('kafka.consumer.fetcher').setLevel(logging.DEBUG)

def on_success(record_meta):
    print(f"Topic: {record_meta.topic}\nPartition: {record_meta.partition}\nOffset: {record_meta.offset}\n")

def on_failure(excep):
    print(excep)

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
    print("polling")
    msg = consumer.poll(1000)
    if msg:
        for topic, records in msg.items():
            for record in records:
                print(f'Topic: {record.topic}, Partion: {record.partition}, Offset: {record.partition}')
                print(f'Message: {record.value}, Key: {record.key}')
