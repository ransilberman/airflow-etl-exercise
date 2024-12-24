# Requirements to run the exercise

## Install sqlite
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


# Exercise:
Change the above code to use realtime data from Yahoo Finance.
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
1. Copy your DAG code (not in pdf format that destroy indentation)
2. Show print-screen of the logs of your DAG run
3. Show the results of a `SELECT` statement that shows the data in the sqlite table
   
