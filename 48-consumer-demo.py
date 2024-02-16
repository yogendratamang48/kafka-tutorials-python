from kafka import KafkaConsumer
import config
import time

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
    group_id = "my-python-app",
    enable_auto_commit=True,
    auto_offset_reset="earliest"
    )

consumer.subscribe([config.TOPIC_NAME])
for msg in consumer:
    print(f"Topic: {config.TOPIC_NAME}, Partition: {msg.partition}, Offset: {msg.offset}, Received message: {msg.value}")
# while True:
#     print("polling")
#     records = consumer.poll(1000)
#     for record in records:
#         print("Record: ", record)
#         print(f'Key: -  {record.key}, Value: {record.value}, Partition: {record.partition}, Offset: {record.partition}')
