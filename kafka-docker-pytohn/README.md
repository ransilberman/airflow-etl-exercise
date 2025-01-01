# kafka-docker-pytohn
Run Kafka with docker and access it via Python

## Kafka documentation:
[KafkaProducer](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html)

Note the following arguments:
- bootstrap_servers
- value_serializer
- acks
- compression_type
- batch_size
- partitioner

[KafkaConsumer](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html)

Note the following arguments:
- bootstrap_servers
- group_id
- value_deserializer
- fetch_max_wait_ms
- fetch_max_bytes
- enable_auto_commit


# Get Kafka in Docker container and run it
Get Kafka Docker container:
```bash
docker pull apache/kafka:latest
```
Run Kafka Docker container:
```bash
docker run -p 9092:9092 apache/kafka:latest
```

# Install kafka-python
```bash
# enter venv:
python3 -m venv .venv
source .venv/bin/activate
pip install kafka-python
```

# Create Consumer
```bash
python
```
```python
from kafka import KafkaConsumer
consumer = KafkaConsumer('my_test_topic')
for msg in consumer:
   print (msg)
```

# Create Producer
```bash
python
```
```python
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
for i in range(100):
    message = "Test message " + str(i)
    encoded_message = message.encode("utf-8")
    producer.send('my_test_topic', encoded_message)
```
