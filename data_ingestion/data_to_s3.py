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
#sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", AWS_ACCESS_ID)
#sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", AWS_ACCESS_KEY)

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

logger = logging.getLogger('spam_application')
logger.setLevel(logging.DEBUG)
logger = logging.basicConfig(level=logging.DEBUG,
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
        logging.info("STATUS: writing file {0} \n".format(destination))
        df_dataset.write.parquet(destination)
        logging.info("STATUS: Completed loading to S3... \n")
        return True
    except Exception as ex:
        logging.exception("ERROR writing to S3 {0}".format(ex))


def get_file_names(base_url, year, dataset_name):
    """
    Generate list of urls for given year
    :param base_url: base url for dataset
    :param year: YYYY
    :return:  [List] of URL
    """
    # base url:
    # get list of file names to download
    file_names = [dataset_name.format(year, month) for month in range(1, 13)]
    # generate full url
    url_list = ["{0}{1}".format(base_url, _name) for _name in file_names]
    return url_list


def load_data_to_S3(year, url_list, bucket_name, destination_address):
    """
    perform data loading from Source to S3
    :return:
    """
    try:
        # create subdirectory in Bucket
        response = s3.put_object(Bucket=bucket_name,
                                 Body='',
                                 Key="{0}/".format(year))

        # if Folder Successful created or Exist StatusCode
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            for url in url_list:
                # step 1: DOWNLOAD DATASET TO EC2
                logging.info("STATUS: downloading dataset... {0}\n".format(url))
                filename = download_dataset(url)
                #print(url, filename)
                # step 2: UPLOAD DATASET TO S3
                if filename:
                    logging.info("STATUS: download completed of dataset {0}".format(filename))

                    logging.info("STATUS: uploading Parquet... \n")
                    destination = destination_address.format(year=year, month=url[-6:-4])
                    upload_to_s3(filename, destination)

                    # step 3: DELETE Dataset
                    logging.info("STATUS: deleteing {0} from local ...\n".format(filename))
                    delete_files(filename)
                else:
                    logging.warning("WARNING: No dataset available...")
                # SLEEP - to avoid being blocked (5mins - 300secs)
                logging.info("STATUS: taking nap zzzzzZZZZZZ....\n")
                time.sleep(60)
        else:
            logging.warning("WARNING : Directory can not be created")
    except Exception as ex:
        logging.exception("ERROR uploading data".format(ex))
    return


def load_submissions(start_year, end_year):
    """
    Load the submissions for start and end period
    :param start_year:
    :param end_year:
    :return:
    """
    logging.info("STATUS: loading submissions .... \n")
    base_url = "https://files.pushshift.io/reddit/submissions/old_v1_data/"
    dataset_name= "RS_{0}-{1:02d}.bz2"
    bucket_name = "reddit-submissions-raw"
    destination_address = "s3a://reddit-submissions-raw/{year}/submissions_{year}_{month}.parquet"
    try:
        for year in range(start_year, end_year + 1):
            # list of url
            url_list = get_file_names(base_url, year, dataset_name)
            logging.info("STATUS: starting to load the data for....{0}".format(year))
            load_data_to_S3(year, url_list, bucket_name, destination_address)
            logging.info("STATUS: Completed loading the data....{0}".format(year))
    except Exception as ex:
        logging.info("error loading the data due to ....{0}".format(ex))
    return


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
    logging.info("STATUS: loading comments .... \n")
    base_url = "https://files.pushshift.io/reddit/comments/"
    dataset_name = "RC_{0}-{1:02d}.bz2"
    bucket_name = "reddit-comments-raw"
    destination_address = "s3a://reddit-comments-raw/{year}/comments_{year}_{month}.parquet"
    try:
        for year in range(start_year, end_year+1):
            # list of url
            url_list = get_file_names(base_url, year, dataset_name)
            logging.info("STATUS: starting to load the data for....{0}".format(year))
            load_data_to_S3(year, url_list, bucket_name, destination_address)
            logging.info("STATUS: Completed loading the data....{0}".format(year))
    except Exception as ex:
        logging.info("error loading the data due to ....{0}".format(ex))
    return


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
    parser.add_argument("--start", "-s",
                        dest="start",
                        required=True,
                        help='year to start data loading eg. 2006')
    parser.add_argument("--end", "-e",
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
