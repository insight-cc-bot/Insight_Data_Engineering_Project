import wget
import boto3
from botocore.exceptions import NoCredentialsError
import os
import sys
import logging
import time
from config import (AWS_ACCESS_ID, AWS_ACCESS_KEY)
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

# spark context
sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", AWS_ACCESS_ID)
sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", AWS_ACCESS_KEY)

# SqlContext
sqlContext = SQLContext(sc)

# create BOTO S3 client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_ACCESS_KEY)


TS = time.strftime("%Y-%m-%d:%H-%M-%S")
log_dir = "./logs/"
if not os.path.exists(log_dir):
    os.mkdir(log_dir)
logger = logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=log_dir+"travel_insights"+str(TS)+".log",
                    filemode='w')

def delete_files(filename):
        try:
            os.system("rm {file}".format(file=filename))
            logger.info("deleted {file} successfully".format(file=filename))
        except Exception as ex:
            logger.warning("not able to delete file due to {0}".format(ex))

def download_dataset(url, year, month):
    # download file - *.bz2 format
    filename = wget.download(url)    
    base_filename = filename[:-4]

    try:
        # load file to spark
        df_comments = sqlContext.read.json(filename)
        # write file to Parquet
        #"s3a://test_system/Output/Test_Result"
        parquet_file_name=
        #destination = "s3n://reddit-comments-raw/year/{0}.parquet".format(filename)
        #df_comments.write.parquet("{0}.parquet".format(filename))
        #destination="s3n://reddit-comments-raw/2006/{0}".format(base_filename)
        file_parquet
        df_comments.write.parquet(destination)
        print("completed loading")        

        return base_filename, filename

    except Exception as ex:
        print(ex)


def get_file_names(base_url, year):
    # base url:
    # get list of file names to download
    file_names = ["RC_{0}-{1:02d}.bz2".format(year, month) for month in range(1,2)]
    # generate full url
    url_list = ["{0}{1}".format(base_url, _name) for _name in file_names]
    return url_list

def load_submissions(year):
    # set base url:
    base_url = ""
    # get file names


def load_comments(year):
    # create subdirectory in Comments S3 Bucket - "reddit-comments-raw"
    response = s3.put_object(Bucket='reddit-comments-raw',
                             Body='',
                             Key='{0}/'.format(year))

    # if successful StatusCode = 200
    # TODO: Validate this logic
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        # set base url:
        base_url = "https://files.pushshift.io/reddit/comments/"
        url_list = get_file_names(base_url, year)
        try:
            for url in url_list:
                # download dataset
                file_to_upload, file_to_delete = download_dataset(url, year, url[-6:-4])
                # upload dataset to S3
                #s3_bucket_name = "{0}/{1}".format(REDDIT_COMMENT_BUCKET, year)
                #upload_to_aws(file_to_upload, s3_bucket_name)
                # delete downloaded file
                if delete_files(file_to_delete):
                    print("successful_delete")
                #TODO: Sleep Time - 1 minute

        except Exception as ex:
            print(ex)
    else:
        return False


def main(year, sub_or_com):
    try:
        if sub_or_com == "submissions":
            # load submissions
            load_submissions(year)
        elif sub_or_com == "comments":
            # load comments
            load_comments(year)
    except Exception as ex:
        print(ex)


if __name__ == "__main__":
    # year of run for submission or comments
    year = sys.argv[1]
    sub_or_com = sys.argv[2]
    main(year, sub_or_com)


"""
import findspark
findspark.init()

# load spark_packages
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

# spark context
sc=SparkContext()

# SparkSQL context
sqlContext = SQLContext(sc)

try:
        temp_df1=sqlContext.read.json('RC_2008-01.bz2')

        filename = "RC_2008-01.parquet"
        destination = "s3a://reddit-comments-raw/2008/{0}".format(filename)#"{0}.parquet".format(filename)

        temp_df1.write.parquet(destination)
        print("done")
except Exception as ex:
        print(ex)
"""
"""
import findspark
findspark.init()

# load spark_packages
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

# spark context
sc=SparkContext()

# SparkSQL context
sqlContext = SQLContext(sc)
filename = "RC_2008-01.parquet"
destination = "s3a://reddit-comments-raw/2008/{0}".format(filename)#"{0}.parquet".format(filename)
print(destination)
try:
        #temp_df1=sqlContext.read.json('RC_2008-01.bz2')

        #filename = "RC_2008-01.parquet"
        #destination = "s3a://reddit-comments-raw/2008/{0}".format(filename)#"{0}.parquet".format(filename)

        #temp_df1.write.parquet(destination)
        data = sqlContext.read.parquet(destination)
        print("done",data.count())
        #print("done")
except Exception as ex:
        print(ex)
        """