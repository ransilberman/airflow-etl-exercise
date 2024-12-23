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
