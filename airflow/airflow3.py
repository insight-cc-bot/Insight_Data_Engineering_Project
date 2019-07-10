"""
# Spark ETL - Generating top topics
"""

from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.macros import ds

# other packages
import os
from datetime import datetime
from datetime import timedelta

# macros
run_date = ds
# year/month for backfill
#YEAR = run_date.year()
# set the date to job execution time
YEAR = '{{ execution_date.strftime("%Y")}}'
MONTH ='{{ execution_date.strftime("%m")}}'

default_arguments = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2010, 7, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    'queue': 'bash_queue',
    "end_date": datetime(2010, 12, 1)
}

spark_cluster = {
    "spark_master": "spark://ec2-**.us-west-2.compute.amazonaws.com:7077",
    "spark_jars": "/usr/local/spark/jars/hadoop-aws-2.7.1.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar",
    "spark_prog": "/home/ubuntu/spark-warehouse/spark_stage2.py",
}
postgres_cluster={
    "postgres_hostname" : "ec2-*.us-west-2.compute.amazonaws.com",
    "postgres_database": "reddit",
    "postgres_table": "wordcount_table",
    "job":"/home/ubuntu/spark-warehouse/S3_to_postgres.py"
}

# Syntax :
# "spark-submit
# --master spark://ec2-*.us-west-2.compute.amazonaws.com:7077
# --jars /usr/local/spark/jars/hadoop-aws-2.7.1.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar
# /home/ubuntu/spark-warehouse/spark1.py arg1 arg2",

get_top_topics = "spark-submit --master {spark_master} --jars {jars} {prog} {arg1} {arg2}".\
    format(spark_master=spark_cluster["spark_master"], jars=spark_cluster["spark_jars"],
           prog=spark_cluster["spark_prog"], arg1=YEAR, arg2=MONTH)

load_to_postgres = "python {prog} {arg1} {arg2} {arg3}".format(prog=postgres_cluster["job"], arg1=YEAR, arg2=MONTH,arg3=MONTH)

# instantiate DAG
dag = DAG(dag_id='popular_topics', description='Get popular words for current month and load to postgres', default_args=default_arguments, schedule_interval='@monthly')

print(get_top_topics)
print(load_to_postgres)

# Task 1: Generate frequent topics
get_words = BashOperator(
    task_id='get_popular_topics',
    bash_command=get_top_topics,
    dag=dag)

# Task 3: Load data to Postgres
load_postgres = BashOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag)

# setting dependencies
load_postgres.set_upstream(get_words)
