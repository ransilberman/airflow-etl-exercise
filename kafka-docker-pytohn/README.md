# kafka-docker-pytohn
Run Kafka with docker and access it via Python

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
