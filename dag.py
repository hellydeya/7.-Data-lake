import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '--- hello ---'
os.environ['YARN_CONF_DIR'] = '--- hello ---'
os.environ['JAVA_HOME']='--- hello ---'
os.environ['SPARK_HOME'] ='--- hello ---'
os.environ['PYTHONPATH'] = '--- hello ---'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 5, 25),
}

dag_spark = DAG(
    dag_id="spark_dag1",
    default_args=default_args,
    schedule_interval=None,
)

t1 = SparkSubmitOperator(
    task_id='t1',
    dag=dag_spark,
    application='--- hello ---',
    conn_id='yarn_spark',
    application_args=[
        "--- hello ---", "3", "3", "--- hello ---", "--- hello ---"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=4,
    executor_memory='2g',
    driver_memory='10g',
    num_executors= 200
)

t1
