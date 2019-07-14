import sys
import os
from elasticsearch import Elasticsearch, helpers

es1 = Elasticsearch([{'host': '10.0.0.6', 'port': 9200}])
es2 = Elasticsearch([{'host': '10.0.0.8', 'port': 9200}])


# elastic search connection
def load_to_elastic(year, filename, dir):
    """
    Perform bulk upload to ElasticSearch in batch size of 400 records (200 recs in ND-JSON)
    """
    batch = list()
    cnt = 0
    es_handle = None
    if year in (2008, 2010):
        es_handle = es1
    else:
        es_handle = es2

    file_to_load = "{0}/{1}".format(dir, filename)
    #print(file_to_load)

    with open(file_to_load,'r') as file_handler:
        print("loading the data for {0} {1}".format(year, month))
        for row in file_handler:
            cnt += 1
            batch.append(row)
            try:
                if len(batch)==400:
                    helpers.bulk(es_handle, index="comments_2008", doc_type="comment", actions=batch, stats_only=True)
                    batch=[]
            except Exception as ex:
                print(ex)
                pass
        print("loaded {0} records for file{1}".format(cnt, file_to_load))
    return


def elastic_search_load(data_dir, year, month):
    if month > 0:
        directory = "{dir}/es_body_{yr}_{mn:02d}".format(dir=data_dir, yr=year, mn=month)
        files_list = [f for f in os.listdir(directory) if f[:4] == "part"]
        # load each file
        for _file in files_list:
            load_to_elastic(year, _file, directory)
    else:
        directory_list = [_dir for _dir in os.listdir(data_dir) if _dir[:2] == "es"]
        for _dir in directory_list:
            try:
                # try to load files in the dir
                directory = "{0}/{1}".format(data_dir, _dir)
                files_list = [f for f in os.listdir(directory) if f[:4] == "part"]
                # load each file
                for _file in files_list:
                    load_to_elastic(year, _file, directory)
            except Exception as ex:
                print(ex)
                pass


if __name__ == '__main__':
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    try:
        dir = "/home/ubuntu/spark-warehouse/Data/{0}".format(year)
        elastic_search_load(dir, year, month)
    except Exception as ex:
        print(ex)
