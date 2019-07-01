import os
import sys

"""
curl -XGET 'http://10.0.0.6:9200/_cluster/health?pretty'
curl -XGET  'http://ec2-54-200-253-96.us-west-2.compute.amazonaws.com:9200/_cluster/health?pretty'

# Cluster 1:10.0.0.6:9200 
curl -XGET  'http://ec2-54-200-253-96.us-west-2.compute.amazonaws.com:9200/_cluster/health?pretty'

# Cluster 1:10.0.0.9:9200
curl -XGET  'http://ec2-34-216-114-106.us-west-2.compute.amazonaws.com:9200/_cluster/health?pretty'
"""
cluster_conf = {"elastic_search_data": "/home/ubuntu/spark-warehouse/Data", "elastic_search_IP1": "10.0.0.6", "elastic_search_port": "9200", "elastic_search_IP2": "10.0.0.8"}


# Step1: get data formatted in bulk load format
def get_formatted_data(year, start, end):
    data_dir = "{0}/{1}".format(cluster_conf['elastic_search_data'], year)
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    for mth in range(start, end+1):
        command = "python spark_elasticsearch.py {0} {1} {2}".format(year, mth, data_dir)

        try:
            print(command)
            os.system(command)
            print("done generating data", year, mth)
        except Exception as ex:
            print(ex)
            pass
    return True


# Step2. Load files to ElasticSearch
def load_to_elastic(filename, files_path):
    port = cluster_conf["elastic_search_port"]
    if year in (2008, 2010, 2011):
        ip_address = cluster_conf["elastic_search_IP1"]
    else:
        ip_address = cluster_conf["elastic_search_IP2"]
    file_to_load = "{0}/{1}".format(files_path, filename)
    es_command = "curl -XPOST http://{ip}:{port}/subscribers/_bulk?pretty --data-binary @{file} -H 'Content-Type: application/json'".format(ip=ip_address, port=port, file=file_to_load)
    print(es_command)
    os.system(es_command)
    return


def elastic_search_load(year):
    data_dir = "{0}/{1}".format(cluster_conf['elastic_search_data'], year)
    directory_list = [_dir for _dir in os.listdir(data_dir) if _dir[:2] == "es"]
    try:
        for _dir in directory_list[:2]:
            files_path = "{0}/{1}".format(data_dir,_dir)
            #print(dir_path)
            files_list = [f for f in os.listdir(files_path) if f[:4] == "part"]
            for _file in files_list:
                load_to_elastic(_file, files_path)
    except Exception as ex:
        print(ex)
        pass


if __name__ == '__main__':
    year = int(sys.argv[1])
    month_s = int(sys.argv[2])
    month_e = int(sys.argv[3])
    # get data formatted in the bulk load format
    try:
        load_status = get_formatted_data(year, month_s, month_e)
        if load_status:
            elastic_search_load(year)
    except Exception as ex:
        print("ERROR:", ex)
