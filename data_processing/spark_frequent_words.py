import json
import sys
import time
import os
import boto3
import logging
from config import (CLEAN_COMMENTS_BUCKET,
                    FREQUENT_WORDS_BUCKET,
                    AWS_ACCESS_KEY_ID,
                    AWS_SECRET_ACCESS_KEY)

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf

from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, trim, col, lower
from pyspark.sql.functions import split, explode, rank, desc
from pyspark.sql.window import Window

# S3  client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# Spark Object
spark = SparkSession.builder.appName('Reddit Comments ETL').getOrCreate()
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)
spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")


log_dir = "./logs/"
if not os.path.exists(log_dir):
    os.mkdir(log_dir)

TS = time.strftime("%Y-%m-%d:%H-%M-%S")
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=log_dir + "data_ingestion" + str(TS) + ".log",
                    filemode='w')


def get_read_file_S3(year, month):
    # file to read from S3
    filename = "{type}_{year}_{month:02d}.parquet".format(type="comments", year=year, month=month)
    source = "s3a://{bucket}/{year}/{file}".format(bucket=CLEAN_COMMENTS_BUCKET, year=year, file=filename)
    return source


def get_write_file_S3(year, month):
    """ generate file name to be read """
    try:
        # create subdirectory in Bucket for year:
        response = s3.put_object(Bucket=FREQUENT_WORDS_BUCKET,
                                 Body='',
                                 Key="{0}/".format(year))

        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            filename = "{type}_{year}_{month:02d}.parquet".format(type="frequent_words", year=year, month=month)
            destination = "s3a://{bucket}/{year}/{file}".format(bucket=FREQUENT_WORDS_BUCKET, year=year, file=filename)
            return destination
        else:
            return None

    except Exception as ex:
        print(ex)


def removePunctuation(column):
    """
    UDF: Removes punctuation, changes to lower case, and strips leading and trailing spaces.
    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.
    Args:
        column (Column): A Column containing a sentence.
    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    return trim(lower(regexp_replace(column, '[^\sa-zA-Z]', ''))).alias('cleaned_body')


def wordCount(wordListDF):
    """Creates a DataFrame with word counts.

    Args:
        wordListDF (DataFrame of str): A DataFrame consisting of one string column called 'word'.

    Returns:
        DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
    """
    return wordListDF.groupBy('word','subreddit_id','subreddit','year', 'month').count()


def elastic_search_mapper_word(df):
    """
    UDF: Generate NDJSON mapping for the dataframe
    :param df:
    :return:
    """
    metadata_json = {"index": {"_index": "comments_{yr}_{m}".format(yr=df.year, m=df.month),
                               "_type": "comment",
                               "_id": df.subreddit_id}}  # TODO: Need a unique ID <subreddit+word maybe>
    word_json = dict()
    word_json["subreddit_id"] = df.subreddit_id
    word_json["subreddit"] = df.subreddit
    word_json["year"] = df.year
    word_json["month"] = df.month
    word_json["word"] = df.word

    final_string = '\n'.join([json.dumps(metadata_json), json.dumps(body_json)])
    return final_string


def generate_frequent_words(filename_read_S3, filename_wite_S3):
    """
    Generate a list of most frequent words for each subreddit.

    :param filename_read_S3: "S3 file location to be read"
    :param filename_wite_S3: "S3 file to write to"
    :return: None
    """
    # get data -
    print("Step 1: read cleaned file into Dataframe from S3")
    comments_df1 = sqlContext.read.parquet(filename_read_S3)

    # select limited columns
    comments_df2 = comments_df1.select('subreddit', 'subreddit_id', 'year', 'month', 'body_without_stopwords')
    print("schema of dataset - {0}".format(comments_df2.printSchema()))

    # -------------------------
    # WORD Count
    # -------------------------
    print("Step  3: Apply punctuation to a body_without_stopwords")
    comments_df3 = comments_df2.select('subreddit', 'subreddit_id', 'year', 'month',
                                       removePunctuation(col('body_without_stopwords')))

    print("Step  4: Split lines to words to generate the count")
    comments_df4 = (comments_df3.select(explode(split(comments_df3.cleaned_body, ' ')).alias('word'),
                                        'subreddit', 'subreddit_id', 'year', 'month').where(col('word') != ''))

    print("Step  5: Get WordCount")
    comments_df5 = wordCount(comments_df4).orderBy("count", ascending=False)

    print("Step  6: Get ranking of each word based on count by windo")
    window = Window.partitionBy(comments_df5['subreddit_id']).orderBy(comments_df5['count'].desc())

    print("Step 7: Get words with rankin > 5")
    comments_df6 = comments_df5.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

    print("writing data to S3")
    # ----------------------
    # Store to S3 - as Parquet
    # ----------------------
    print("Step 8: Generate parquet file for the words and load to S3")
    comments_df6.write.parquet(filename_wite_S3)
    print("Completed writing data to S3")
    return


def main(year, month):
    # get file name to read from S3
    file_from_S3=get_read_file_S3(year, month)

    # get file name to write to S4
    file_to_S3 = get_write_file_S3(year, month)

    try:
        # Get frequent words
        generate_frequent_words(file_from_S3, file_to_S3)
        print("SUCCESFULL Completed the word calculation")
        return
    except Exception as ex:
        logging.exception("ERROR occured - {0}".format(ex))
        raise


if __name__ == "__main__":
    input_year = int(sys.argv[1])
    input_month = int(sys.argv[2])
    main(input_year, input_month)
