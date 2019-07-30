"""
# Spark ETL DAG
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
import os
import shutil
from datetime import datetime
from datetime import timedelta

#YEAR = 2008
#MONTH = 5
YEAR = '{{ execution_date.strftime("%Y")}}'
MONTH ='{{ execution_date.strftime("%m")}}'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2012, 1, 1),
    "end_date": datetime(2012, 12, 1),
    "email": ["mehta.dhananjay28@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,    
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

cluster_conf = {
    "elastic_search_data": "/home/ubuntu/Data",
    "elastic_search_IP": "10.0.0.13",
    "elastic_search_port": "9200",
    "spark_master": "spark://ec2-34-211-145-63.us-west-2.compute.amazonaws.com:7077",
    "spark_jars": "/usr/local/spark/jars/hadoop-aws-2.7.1.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar",
    "spark_prog": "/home/ubuntu/spark-warehouse/spark_stage1.py"
}


# ------- Airflow ------
# step 1: Set Directory structure
def set_dir_structure():
    data_dir = cluster_conf["elastic_search_data"]
    # delete the folders if path exist
    print(data_dir)
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)


# Step 2. Run the spark job : Bash Task
# "spark-submit
# --master spark://ec2-34-211-145-63.us-west-2.compute.amazonaws.com:7077
# --jars /usr/local/spark/jars/hadoop-aws-2.7.1.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar
# /home/ubuntu/spark-warehouse/spark1.py arg1 arg2",
spark_bash_job = "spark-submit --master {spark} --jars {jars} {prog} {arg1} {arg2}".format(
    spark=cluster_conf["spark_master"],
    jars=cluster_conf["spark_jars"], prog=cluster_conf["spark_prog"], arg1=YEAR, arg2=MONTH)

# Step 3. Load files to ElasticSearch
def load_to_elastic(filename):
    ip_address = cluster_conf["elastic_search_IP"]
    port = cluster_conf["elastic_search_port"]

    es_command = "curl -XPOST http://{ip}:{port}/subscribers/_bulk?pretty " \
              "--data-binary @{file} -H 'Content-Type: application/json'".format(ip=ip_address, port=port,
                                                                                 file=filename)
    try:
        #output = os.system("{command}".format(command=es_command))
        print(es_command)
    except Exception as ex:        
        pass
    return


def elastic_search_load():
    # get folders in directory
    directory_list = [_dir for _dir in os.listdir(cluster_conf['elastic_search_data'])
                      if (os.path.isdir(_dir) and _dir[:2] == "es")]
    try:
        # get list of files in each dir
        for _dir in directory_list:
            files_list = [_file for _file in os.listdir(_dir) if (os.path.isfile(os.path.join(_dir, _file)) and _file[:4] == "part")]
            # traverse each file
            for file in files_list:
                # Load each file to elastic
                load_to_elastic(file)
    except Exception as ex:        
        pass


# instantiate DAG
#dag = DAG(dag_id='spark_etl', description='NLP Data Cleaning and indexing', default_args=default_args, schedule_interval=timedelta(days=1))
dag = DAG(dag_id='spark_etl', description='NLP Data Cleaning and indexing', default_args=default_args, schedule_interval='@monthly')

# set tasks
dir_job = PythonOperator(
    task_id='set_directory_structure',
    python_callable=set_dir_structure,
    dag=dag)

spark_job = BashOperator(
    task_id='spark_etl_job',
    bash_command=spark_bash_job,
    dag = dag)

es_job = PythonOperator(
    task_id='load_to_elastic',
    python_callable=elastic_search_load,
    dag=dag)

# setting dependencies
spark_job.set_upstream(dir_job)
es_job.set_upstream(spark_job)
