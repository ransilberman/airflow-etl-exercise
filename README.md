

# Install and Run Airflow
Follow the instructions described in [install-airflow-on-aws](https://github.com/ransilberman/airflow-etl-exercise/blob/main/install-airflow-on-aws%20/README.md)

# Install sqlite
```bash
sudo apt install sqlite3
sqlite3 /tmp/airflow_example.db
```
## Create table tweets
```sql
sqlite> CREATE TABLE IF NOT EXISTS tweets(
      id SERIAL PRIMARY KEY,
      datetime DATE NOT NULL,
      username VARCHAR(200) NOT NULL,
      text TEXT,
      source VARCHAR(200),
      location VARCHAR(200)
);

sqlite> .tables
tweets
sqlite> .exit
```

# Create DAG

Dag Code: `taskflow_x_sqlite_dag.py`

```python

from __future__ import annotations
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import pyarrow as pa
import sqlite3

# Define the database file (local SQLite database)
DB_FILE = "/tmp/airflow_example.db"

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Exercize"],
)

def taskflow_x_sqlite():

    @task()
    def extract():
        current_datetime = datetime.now()
        tweets_list = [[current_datetime, "Joe Dow", "Great news", "www.cnn.com", "USA"], \
                [current_datetime, "Jane Dow", "Better news","www.fox.com", "USA"], \
                [current_datetime, "Moses Cohen", "Even Greater News", "www.walla.co.il", "Israel"]]
        tweets_df = pd.DataFrame(tweets_list, columns=['datetime', 'username', 'text', 'source', 'location'])
        return tweets_df

    @task()
    def transform(tweets_df:pd.DataFrame):
        filtered_df = tweets_df[tweets_df['location'] == 'USA']
        print(filtered_df)
        return filtered_df

    @task()
    def load(filtered_df:pd.DataFrame):
        """Function to insert data into the SQLite database."""
        conn = sqlite3.connect(DB_FILE)
        filtered_df.to_sql('tweets', conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()

    # [START main_flow]
    load(transform(extract()))
    # [END main_flow]        

# [START dag_invocation]
taskflow_x_sqlite()
# [END dag_invocation]

```

# Start Airflow
Install the following libraries for Python:
```bash
pip install pandas
pip install pyarrow
```
1. Stop Airflow if running (Ctrl+C)
2. Start airflow again by running `airflow standalone'
3. Open Airflow GUI in [http://localhost:8080](http://localhost:8080)
4. Look for the DAG `taskflow_x_sqlite`
   

# Exercises:

## Exercise-1: Add Kafka-consumer to a task in the DAG
1. Install Kafka on Docker locally. See [kafka-docker-pytohn](https://github.com/ransilberman/airflow-etl-exercise/blob/main/kafka-docker-pytohn/README.md)
2. Use the code below to replcace the 'extract' task in the DAG you wrote above:
```python
# use this code to replace the 'extract' task:
@task()
def extract():
   # Kafka configuration
   KAFKA_BROKER_URL = "localhost:9092"
   KAFKA_TOPIC = "my_test_topic"
   MAX_CONSUME_MESSAGES = 10
   current_datetime = datetime.now()
   consumer = KafkaConsumer(
       KAFKA_TOPIC,
       bootstrap_servers=KAFKA_BROKER_URL,
       auto_offset_reset="earliest",
       enable_auto_commit=True,
   )
   messages = consumer.poll(timeout_ms=1000, max_records=MAX_CONSUME_MESSAGES)
   # Process messages
   tweets_list_kafka=list()
   for topic_partition, records in messages.items():
       for record in records:
           tweets_list_kafka.append([current_datetime, "Joe Dow", record.value.decode("utf-8"), "www.cnn.com", "USA"])
   print(tweets_list_kafka)
   tweets_df = pd.DataFrame(tweets_list_kafka, columns=['datetime', 'username', 'text', 'source', 'location'])
   return tweets_df
```
3. Generate some tweets and see that they are stored in sqlite tabe.

## Exercise-2: Write your first DAG
Based on the above code, write a new DAG to use realtime data from Yahoo Finance.
This DAG will read the finance quotes from Yahoo and store it in a new table in SQLite table.
Use the following code snippet to implement reading yahoo quotes for Apple stock:
```bash
pip install yfinance
```

```python
import yfinance as yf

# Fetch stock data
stock = yf.Ticker("AAPL")  # Example: Apple Inc.
stock_info = stock.info

print(f"Name: {stock_info['longName']}")
print(f"Symbol: {stock_info['symbol']}")
print(f"Current Price: {stock_info['regularMarketPrice']}")

# Get historical data
data = stock.history(period="1mo")  # Options: '1d', '5d', '1mo', '6mo', '1y', etc.
print(data)
```

## Requirements for submittion the exercise:
1. Copy your DAGs code (not in pdf format that destroy indentation)
2. Show print-screen of the logs of your DAGs run
3. Show the results of the `SELECT` statements that shows the data in the sqlite tables.
   
