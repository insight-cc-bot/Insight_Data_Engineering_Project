import os
import sys
import timeit
import json

cluster_conf = {
    "elastic_search_data": "/home/ubuntu/Data",
    "elastic_search_IP": "10.0.0.13",
    "elastic_search_port": "9200",
    "spark_master": "spark://ec2-34-211-145-63.us-west-2.compute.amazonaws.com:7077",
    "spark_jars": "/usr/local/spark/jars/hadoop-aws-2.7.1.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar",
    "spark_prog": "/home/ubuntu/spark-warehouse/spark1.py"
}

def execute_spark(spark_job):
    try:
        os.system(spark_job)
        #print(spark_job)
        return True
    except Exception as ex:
        return False


def main(year_start, month_start, month_end):
    # measure performance of job by clocking run-time
    performance = {}
    for _year in range(year_start, year_start+1):
        for _month in range(month_start,month_end+1):
            spark_bash_job = "spark-submit --master {spark} --jars {jars} {prog} {arg1} {arg2}".\
                format(spark=cluster_conf["spark_master"], jars=cluster_conf["spark_jars"],
                       prog=cluster_conf["spark_prog"], arg1=_year, arg2=_month)
            try:
                start_time = timeit.default_timer()
                execute_spark(spark_bash_job)
                end_time = timeit.default_timer()
                time_taken = start_time-end_time
                period = "{0}_{1}".format(_year,_month)
                performance[period]=(_year, _month, time_taken)
            except Exception as ex:
                continue

    # path to load file
    json_oputput = cluster_conf['elastic_search_data']+"performance_{0}".format(year_start)
    
    if json_oputput:
        # Writing JSON data
        with open(json_oputput, 'w') as f:
            json.dump(performance, f)
    return performance

if __name__=="__main__":
    year_s=int(sys.argv[1])
    month_s=int(sys.argv[2])
    month_e=int(sys.argv[3])
    main(year_s, month_s, month_e)
