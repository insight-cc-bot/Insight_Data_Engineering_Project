import wget
import boto3
from botocore.exceptions import NoCredentialsError
from argparse import ArgumentParser
import os
import sys
import logging
import time

from config import (AWS_ACCESS_ID, AWS_ACCESS_KEY)

# ===== SPARK Configs =====
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SQLContext

# spark context
sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", AWS_ACCESS_ID)
sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", AWS_ACCESS_KEY)

# SqlContext
sqlContext = SQLContext(sc)

# ===== AWS: create BOTO S3 client =====
s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_ID,
                  aws_secret_access_key=AWS_ACCESS_KEY)

# ==== Logging =====
TS = time.strftime("%Y-%m-%d:%H-%M-%S")
log_dir = "./logs/"
if not os.path.exists(log_dir):
    os.mkdir(log_dir)

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=log_dir + "data_ingestion" + str(TS) + ".log",
                    filemode='w')


def delete_files(filename):
    """
    Delete downloaded file
    :param filename: file to be deleted
    :return: None
    """
    try:
        os.system("rm {file}".format(file=filename))
        logging.info("deleted {file} successfully".format(file=filename))
        return
    except Exception as ex:
        logging.info("ERROR able to delete file due to {0}".format(ex))


def download_dataset(url):
    """
    - Download the dataset in .bz2 format
    :param url: dataset url to be downloaded
    :return: downloaded filename
    """
    # download file
    try:
        filename = wget.download(url)
        logging.info("downloaded {0}".format(filename))
        return filename
    except Exception as ex:
        logging.info("ERROR downloading the file {err}".format(err=ex))


def upload_to_s3(filename, destination):
    """
    - Load downloded bz2 file to Spark
    - Dump in S3 as parquet from Spark
    :param filename: File to be read
    :param destination: S3 destination address
    :return:
    """
    try:
        # load file to spark
        df_dataset = sqlContext.read.json(filename)
        # write file to Parquet
        print("STATUS: writing file {0} \n".format(destination))
        df_dataset.write.parquet(destination)
        print("STATUS: Completed loading to S3... \n")
        return True
    except Exception as ex:
        logging.exception("ERROR writing to S3 {0}".format(ex))


def get_file_names(base_url, year):
    """
    Generate list of urls for given year
    :param base_url: base url for dataset
    :param year: YYYY
    :return:  [List] of URL
    """
    # base url:
    # get list of file names to download
    file_names = ["RC_{0}-{1:02d}.bz2".format(year, month) for month in range(1, 13)]
    # generate full url
    url_list = ["{0}{1}".format(base_url, _name) for _name in file_names]
    return url_list

# TODO
def load_submissions(start_year, end_year):
    # set base url:
    base_url = ""
    # get file names


def load_comments(start_year, end_year):
    """
    Load comments for given range of years.

        - Step 1: Get list of URLs for a year, 1 URL for each month each year
        - Step 2: Download dataset in RC_YYYY-MM.bz2 format
        - Step 3: Load dataset to Spark and offload as Parquet on S3
        - Step 4: Delete the downloaded file

    :param start_year: YYYY Starting year of Upload
    :param end_year: YYYY Ending year of Upload
    :return:
    """
    print("STATUS: loading comments .... \n")
    try:
        # load data for input range of years:
        for year in range(start_year, end_year + 1):
            # Step 1: get dataset URLs
            base_url = "https://files.pushshift.io/reddit/comments/"
            url_list = get_file_names(base_url, year)
            print("STATUS: loading url list ...\n")
            # Step 2: create subdirectory in Comments S3 Bucket "reddit-comments-raw"
            response = s3.put_object(Bucket='reddit-comments-raw',
                                     Body='',
                                     Key="{0}/".format(year))

            # if Folder Successful created or Exist StatusCode
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                for url in url_list:
                    # DOWNLOAD DATASET TO EC2
                    print("STATUS: downloading dataset... \n")
                    filename = download_dataset(url)
                    print("STATUS: download completed{0}".format(filename))
                    # UPLOAD DATASET TO S3
                    if filename:  # if it doesn't return None
                        print("STATUS: loading Parquet... \n")
                        destination = "s3a://reddit-comments-raw/{year}/comments_{year}_{month}.parquet".\
                            format(year=year, month=url[-6:-4])
                        upload_to_s3(filename, destination)
                        print("STATUS: deleteing {0} from local ...\n".format(filename))
                        # DELTE DATASET FROM EC2
                        delete_files(filename)
                        # SLEEP - to avoid being blocked (5mins - 300secs)
                    print("STATUS: taking nap zzzzzZZZZZZ....\n")
                    time.sleep(60)
            else:
                logging.warning("WARNING : Directory can not be created")
        print("STATUS: Completed loading the data....{0}".format(year))
        return True
    except Exception as ex:
        logging.exception("ERROR uploading data".format(ex))


def main(start_year, end_year, sub_or_com):
    """
    Based on user input decide if Comments or Submissions need to be processed
    :param start_year:
    :param end_year:
    :param sub_or_com:
    :return:
    """
    try:
        if sub_or_com == "submissions":
            # load submissions
            load_submissions(start_year, end_year)
            logging.info("Loading completed for submissions between{0}{1}".format(start_year, end_year))

        elif sub_or_com == "comments":
            # load comments
            load_comments(start_year, end_year)
            logging.info("Loading completed for comments between{0}{1}".format(start_year, end_year))
        else:
            logging.exception("Incorrect Option for loading")
    except Exception as ex:
        logging.exception("ERROR loading dataset for {0} for range {1}{2}".format(ex, start_year, end_year))


if __name__ == "__main__":
    parser = ArgumentParser(description='Upload dataset to S3')
    parser.add_argument("--start_year", "-start",
                        dest="start",
                        required=True,
                        help='year to start data loading eg. 2006')
    parser.add_argument("--end_year", "-end",
                        dest="end",
                        required=True,
                        help='year till which data needs uploading eg. 2009')
    parser.add_argument("--type", "-t",
                        dest="category",
                        required=True,
                        help='submissions or comments eg. submissions')

    parameters = parser.parse_args()

    # year of run for submission or comments
    start_year = int(parameters.start)
    end_year = int(parameters.end)
    sub_or_com = parameters.category

    try:
        # Start execution
        main(start_year, end_year, sub_or_com)        
        logging.info("SUCCESS completed loading {0}".format(sub_or_com))
    except Exception as ex:
        logging.info("FAILURE failed to load{0}".format(ex))
