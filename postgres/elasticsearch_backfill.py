import os

cluster_conf = { "elastic_search_data": "/home/ubuntu/spark-warehouse/Data", "elastic_search_IP": "10.0.0.13", "elastic_search_port": "9200"}

# Step 3. Load files to ElasticSearch
def load_to_elastic(filename):
    ip_address = cluster_conf["elastic_search_IP"]
    port = cluster_conf["elastic_search_port"]

    es_command = "curl -XPOST http://{ip}:{port}/subscribers/_bulk?pretty --data-binary @{file} -H 'Content-Type: application/json'".format(ip=ip_address, port=port, file=filename)
    """
    try:
        output = os.system("{command}".format(command=es_command))
    except Exception as ex:
        pass
    """
    print(es_command)
    return


def elastic_search_load():
    # get folders in directory
    directory_list = [_dir for _dir in os.listdir(cluster_conf['elastic_search_data'])
                      if (os.path.isdir(_dir) and _dir[:2] == "es")]
    try:
        # get list of files in each dir
        for _dir in directory_list[:2]:
            files_list = [_file for _file in os.listdir(_dir) if (os.path.isfile(os.path.join(_dir, _file)) and _file[:4] == "part")]
            # traverse each file
            for file in files_list:
                # Load each file to elastic
                load_to_elastic(file)
    except Exception as ex:
        pass


if __name__ == '__main__':
  elastic_search_load()
