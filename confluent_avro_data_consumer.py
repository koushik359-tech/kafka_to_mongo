# Make sure to install confluent-kafka python package
# pip install confluent-kafka

import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import os
import json
from pymongo import MongoClient



# MongoDB configuration
# MongoDB configuration
mongo_client = MongoClient('mongodb+srv://k1:Abcd1234@cluster0.ppen2ob.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')  # Replace with your MongoDB connection string
db = mongo_client['gds_db']  # Replace with your database name
collection = db['retail_data']  # Replace with your collection name








# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'Y7OSGORXPWZQHAOP',
    'sasl.password': 'jzAGhIu/NIdoGUhmJb9qdMsN/mnUtqjYQWZ3n+ELLdslCqlAZn9jp8cB1ioHVpNm',
    'group.id': 'group2',
    'auto.offset.reset': 'latest'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-qjmzd.us-east-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('WB3KRNHWUPAQRAY6','Gt6UPlcS2eY/CYFdKOQShLuGjAGZXmbkNnCandOVzFQrsGJ9vSY0E3XSIFgYhVNM')
})

# Fetch the latest Avro schema for the value
subject_name = 'topic_1-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'retail_data' topic
consumer.subscribe(['topic_1'])

#Continually read messages from Kafka

while True:
    msg = consumer.poll(1.0) # How many seconds to wait for message

    if msg is None:
        continue
    if msg.error():
        print('Consumer error: {}'.format(msg.error()))
        continue

    print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))
    
    value=msg.value()
    collection.insert_one(value)
    print("Inserted message into MongoDB:", value)



# except KeyboardInterrupt:
#     pass
# finally:
#     consumer.commit()
#     consumer.close()
#     mongo_client.close()
#     # In[ ]:



# consumer.close()