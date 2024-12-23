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

Dag Code: `etl_x_sqlite_dag.py`

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3

# Define the database file (local SQLite database)
DB_FILE = "/tmp/airflow_example.db"

def insert_data():
    """Function to insert data into the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    current_datetime = datetime.now()
    cursor.execute("""
        INSERT INTO tweets (datetime, username, text, source, location)
        VALUES (?, ?, ?, ?, ?)
    """, (current_datetime, "Ran S", "Some Text", "cnn.com", "USA"))

    conn.commit()
    conn.close()

def fetch_data():
    """Function to fetch and log data from the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tweets")
    rows = cursor.fetchall()
    for row in rows:
        print(row)  # This will appear in the task logs
    conn.close()

# Define the DAG
with DAG(
    dag_id="etl_x_sqlite_dag",
    start_date=datetime(2024, 12, 20),
    schedule_interval=None,  # Trigger manually
    catchup=False,
) as dag:

    # Task to insert data
    insert_data_task = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data,
    )

    # Task to fetch data
    fetch_data_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
    )

    # Define task dependencies
    insert_data_task >> fetch_data_task

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
1. copy your DAG code (not in pdf format that destroy indentation)
2. send pring screen of the logs of your DAG run
3. select the results of a SELECT statement that shows the data in the sqlite table
   
