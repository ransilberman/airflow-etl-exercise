# install-airflow-on-aws
How to install and Run Standalone Apache Airflow on EC2

# Run in a venv
```bash
sudo apt-get update; sudo apt install python3.10-venv
python3 -m venv .venv
source .venv/bin/activate
```

# Set environment
```bash
AIRFLOW_VERSION=2.10.4
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
echo $PYTHON_VERSION
CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt
sudo apt-get update; sudo apt-get install python3-pip
pip3 install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
cd; mkdir ~/airflow; mkdir ~/airflow/dags
```

# Start Airflow Standalone
```bash
export AIRFLOW_HOME=~/airflow
export PATH="/home/ubuntu/.local/bin:$PATH"
airflow standalone
```

# View Console
Look in the log to the lines starting with “standalone” Find the string: `Login with username: admin  <password>:  ???? ` in the console.
Open URL : [http://localhost:8080](http://localhost:8080)
Enter user: `admin` + `<password>`
Enable `example_bash_operator`

# Test Task
```bash
# run your first task instance 
export AIRFLOW_HOME=~/airflow
export PATH="/home/ubuntu/.local/bin:$PATH"
airflow tasks test example_bash_operator runme_0 2024-12-19
```
<img width="872" alt="image" src="https://github.com/user-attachments/assets/2ac87cc7-6464-4553-bb7f-c52a08db505e" />

```
Output:
INFO - Running command: ['/bin/bash', '-c', 'echo "example_bash_operator__runme_0__20241219" && sleep 1’]
INFO - Output:
INFO - example_bash_operator__runme_0__20241219
INFO - Command exited with return code 0
```
![image](https://github.com/user-attachments/assets/47a90184-dba6-4d5a-ad23-32a28abc44b0)


# Run DAG
```bash
# trigger DAG from CLI
airflow dags trigger example_bash_operator

```
<img width="862" alt="image" src="https://github.com/user-attachments/assets/48cfb36b-85a7-46e5-8e7d-4e61d95c4956" />

# Create your user-defined DAG
1. Create your own Python DAG file.
2. Place the file under `~/airflow/dags`
3. Run the command `airflow dags list` and check that you see your DAG.
4. Check that it appears in the UI and enable it.
