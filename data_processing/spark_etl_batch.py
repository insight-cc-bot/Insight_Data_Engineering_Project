import boto3
import json
import os
import time
import logging
import sys
from stopwords import gen_custom_stop_words
CUSTOM_STOPWORDS = gen_custom_stop_words()
from config import (RAW_COMMENTS_BUCKET,
                    CLEAN_COMMENTS_BUCKET,
                    OUTPUT_DIRECTORY,
                    AWS_ACCESS_KEY_ID,
                    AWS_SECRET_ACCESS_KEY)

import findspark
findspark.init()

# load spark_packages
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

# creating user defined function
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType

# sql functions for trimming
from pyspark.sql.functions import regexp_replace, trim, col, lower

# for converting utc timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import year, month

# Spark SQL
from pyspark.sql.functions import *
from pyspark.sql.functions import col, size
from pyspark.sql.functions import split, explode
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

# Spark ML - NLP functions
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import NGram

# ===== AWS: create BOTO S3 client =====
s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# ===== Logging ========
TS = time.strftime("%Y-%m-%d:%H-%M-%S")
log_dir = "./logs/"
if not os.path.exists(log_dir):
    os.mkdir(log_dir)

logger = logging.getLogger('spam_application')
logger.setLevel(logging.DEBUG)
logger = logging.basicConfig(level=logging.DEBUG,
                             format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                             datefmt='%m-%d %H:%M',
                             filename=log_dir + "ETL_Pipeline" + str(TS) + ".log",
                             filemode='w')
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('py4j').setLevel(logging.INFO)  # use setLevel(logging.ERROR) is also fine
logging.getLogger('pyspark')

logging.info('Task is successful.')

# === Set Spark Configs ===
spark = SparkSession.builder.appName('Reddit Comments ETL').getOrCreate()
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)
spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")

# === Set Data Dir ====
data_dir = "./Data/"
if not os.path.exists(data_dir):
    os.mkdir(data_dir)


# TODO: get filename for S3
def get_read_file_S3(year, month=None):
    """ generate file name to be read """
    if month:
        filename = "{type}_{year}_{month:02d}.parquet".format(type="comments", year=year, month=month)
        destination = "s3a://{bucket}/{year}/{file}".format(bucket=RAW_COMMENTS_BUCKET, year=year, file=filename)
    else:
        destination = "s3a://{bucket}/{year}/*".format(bucket=RAW_COMMENTS_BUCKET, year=year)

    return destination


def get_write_file_S3(year, month=None):
    """ generate file name to be read """
    try:
        # create subdirectory in Bucket for year:
        response = s3.put_object(Bucket=CLEAN_COMMENTS_BUCKET,
                                 Body='',
                                 Key="{0}/".format(year))
        
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            filename = "{type}_{year}_{month:02d}.parquet".format(type="comments", year=year, month=month)
            destination = "s3a://{bucket}/{year}/{file}".format(bucket=CLEAN_COMMENTS_BUCKET, year=year, file=filename)
            return destination
        else:
            return None

    except Exception as ex:
        logging.info(ex)


def trim_link(link):
    """
    UDF: to trim columns
    :param link:
    :return:
    """
    return link[3:]


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


def filter_stop_words(token_list):
    """
    UDF: to filter additional stopwords
    :param token_list:
    :param stopwords:
    :return:
    """
    return [word for word in token_list if word not in CUSTOM_STOPWORDS]


def wordCount(wordListDF):
    """
    UDF: Creates a DataFrame with word counts.

    Args:
        wordListDF (DataFrame of str): A DataFrame consisting of one string column called 'word'.

    Returns:
        DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
    """
    return wordListDF.groupBy('word', 'subreddit_id', 'subreddit', 'year', 'month').count()


def elastic_search_mapper_body(df):
    """
    UDF: Generate NDJSON mapping for the dataframe
    :param df:
    :return:
    """
    metadata_json = {"index": {"_index": "comments_{yr}_{m}".format(yr=df.year, m=df.month),
                               "_type": "comment",
                               "_id": df.id}}
    body_json = dict()
    body_json["id"] = df.id
    body_json["author"] = df.author
    body_json["subreddit"] = df.subreddit.lower()
    body_json["subreddit_id"] = df.subreddit_id
    body_json["body"] = df.body
    body_json["year"] = int(df.year)
    body_json["month"] = int(df.month)
    body_json["day"] = int(df.day)
    body_json["score"] = int(df.score)
    body_json["controversiality"] = int(df.controversiality)
    body_json["ups"] = int(df.ups)
    body_json["downs"] = int(df.downs)

    final_string = '\n'.join([json.dumps(metadata_json), json.dumps(body_json)])
    return final_string


# TODO: Main ETL Pipeline for Data
def spark_transformation_comments(filename_read_S3, filename_write_elastic, filename_write_S3):
    """
    Columns in Input:
    'archived', 'author', 'author_flair_css_class', 'author_flair_text', 'body', 'controversiality', 'created_utc',
     'distinguished', 'downs', 'edited', 'gilded', 'id', 'link_id', 'name', 'parent_id', 'retrieved_on', 'score',
     'score_hidden', 'subreddit', 'subreddit_id', 'ups'

    :param filename_read_S3: File to read from
    :param filename_write_elastic: Output file for Elastic
    :param filename_write_S3: Cleaned files to S3
    :return:
    """

    # ---------------------------------------------
    # -------- BASIC TRANSFORMATIONS --------
    # ----------------------------------------------
    logging.info("Stage 1: read file into Dataframe from S3")
    comments_df1 = sqlContext.read.parquet(filename_read_S3)

    columns = comments_df1.columns
    logging.info("List of columns for Comments - {0}".format(columns))

    logging.info("Stage 2: select required columns from data")
    # NOTE: Column "Downs", 'name' isn't available for all years
    # Its available only from 2006-06
    if 'downs' in columns and 'name' in columns:
        comments_df2 = comments_df1.select('subreddit', 'subreddit_id', 'created_utc', 'author', 'id', 'link_id',
                                           'parent_id', 'body', 'controversiality', 'distinguished', 'gilded',
                                           'score', 'ups', 'downs', 'name')
    else:
        comments_df2 = comments_df1.select('subreddit', 'subreddit_id', 'created_utc', 'author', 'id', 'link_id',
                                           'parent_id', 'body', 'controversiality', 'distinguished', 'gilded',
                                           'score', 'ups')

    logging.info("Stage 3: Removing rows where post has been deleted , #TODO: Need to include removed")
    comments_df3 = comments_df2.filter(comments_df2['author'] != '[deleted]')

    # Create and Register trim_link as UDF
    spark.udf.register("trimlinks", trim_link, StringType())
    trim_link_udf = udf(trim_link)

    logging.info("Stage 4: get submission_id from link_id")
    comments_df4 = comments_df3.withColumn("submission_id", trim_link_udf(col("link_id")))

    # ---------------------------------------------
    # -------- FEATURE ENGINEERING ---------------
    # ---------------------------------------------
    logging.info("Stage 5: Convert 'created_uct' to unix timestamp")
    comments_df5 = comments_df4.select('subreddit', 'subreddit_id', 'author', 'id', 'parent_id', 'body',
                                       'controversiality', 'distinguished', 'gilded', 'score', 'ups',
                                       'downs', 'name', 'submission_id',
                                       from_unixtime('created_utc').alias('timestamp'))

    logging.info("Stage 6: Add new features: Year, Month, day, hour, minute, week, julian day")
    comments_df6 = comments_df5.select('subreddit', 'subreddit_id', 'author', 'id', 'parent_id', 'body',
                                       'controversiality', 'distinguished', 'gilded', 'score', 'ups', 'downs', 'name',
                                       'submission_id', year(comments_df5.timestamp).alias('year'),
                                       month(comments_df5.timestamp).alias('month'),
                                       dayofmonth(comments_df5.timestamp).alias('day'),
                                       dayofyear(comments_df5.timestamp).alias('day_of_year'),
                                       hour(comments_df5.timestamp).alias('hour'),
                                       minute(comments_df5.timestamp).alias('min'),
                                       weekofyear(comments_df5.timestamp).alias('week_of_year'))

    # ---------------------------------------
    #   PERSIST Data for following reasons:
    # ---------------------------------------
    # 1. Write data to ElasticSearch after ETL
    # 2. Perform NLP based data cleaning for comments
    # 3. Identify popular words
    # 4. Load NLP cleaned data to S3
    # 5. Load Words to ElasticSearch
    comments_df6.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logging.info("persisted data after initial cleaning")

    # -------------------------------------------
    # Write to ElasticSearch: NDJSON file
    # -------------------------------------------
    # Load the Cleaned data to ElasticSearch
    logging.info("starting transforming data to NDJSON - for Large ES load")
    nd_json = comments_df6.rdd.map(lambda x: elastic_search_mapper_body(x))
    logging.info("completed transformation to NDJSON")

    logging.info("save data as Text file")
    if not os.path.exists(filename_write_elastic):
        nd_json.saveAsTextFile(filename_write_elastic)
    else:
        logging.info("data already loaded")

    # ----------------------------
    # NLP transformations Pipeline
    # -----------------------------
    logging.info("Stage 7: Remove Punctuations")
    comments_df7 = comments_df6.select('subreddit', 'subreddit_id', 'author', 'id', 'parent_id', 'body',
                                       'controversiality', 'distinguished', 'gilded', 'score', 'ups', 'downs', 'name',
                                       'submission_id', 'year', 'month', 'day', 'day_of_year', 'hour', 'min',
                                       'week_of_year', removePunctuation(col('body')))

    logging.info("stage 8: Word Tokenization")
    tokenizer = Tokenizer(inputCol="cleaned_body", outputCol="tokenized_body")
    comments_df8 = tokenizer.transform(comments_df7).select('subreddit', 'subreddit_id', 'author', 'id', 'parent_id',
                                                            'body', 'controversiality', 'distinguished', 'gilded',
                                                            'score', 'ups', 'downs', 'name', 'submission_id', 'year',
                                                            'month', 'day', 'day_of_year', 'hour', 'min',
                                                            'week_of_year', 'tokenized_body')

    # StopWords Removal
    logging.info("Stage 9: Using SPARK default stopwords.")
    remover = StopWordsRemover()
    remover.setInputCol("tokenized_body")
    remover.setOutputCol("no_stop_words_body")
    comments_df9 = remover.transform(comments_df8).select('subreddit', 'subreddit_id', 'author', 'id', 'parent_id',
                                                          'body', 'controversiality', 'distinguished', 'gilded',
                                                          'score', 'ups', 'downs', 'name', 'submission_id', 'year',
                                                          'month', 'day', 'day_of_year', 'hour', 'min', 'week_of_year',
                                                          'no_stop_words_body')
    logging.info("Stage 10: Making a Custom list of words")
    # TODO: Get Reddit frequent words
    spark.udf.register("filterExtraStopWords", filter_stop_words, ArrayType(StringType()))
    filter_stop_words_udf = udf(filter_stop_words)
    comments_df10 = comments_df9.select('subreddit', 'subreddit_id', 'author', 'id', 'parent_id',
                                        'body', 'controversiality', 'distinguished', 'gilded',
                                        'score', 'ups', 'downs', 'name', 'submission_id', 'year',
                                        'month', 'day', 'day_of_year', 'hour', 'min', 'week_of_year',
                                        filter_stop_words_udf("no_stop_words_body").alias("body_without_stopwords"))

    logging.info(comments_df10.printSchema())
    # -------------------------
    # Upload Cleaned data to S3
    # -------------------------
    comments_df10.write.parquet(filename_write_S3)
    logging.info("completed loading the data to S3")


def main(year, month):
    # get data from S3 for the requested year / year-month
    filename_read = get_read_file_S3(year, month)
    logging.info(filename_read)

    filename_write_S3 = get_write_file_S3(year, month)

    logging.info(filename_write_S3)

    filename_write_elastic = "{dir}/es_body_{year}_{month:02d}.txt".format(dir=OUTPUT_DIRECTORY, year=year, month=month)

    try:
        # start ETL
        spark_transformation_comments(filename_read, filename_write_elastic, filename_write_S3)
        logging.info("SUCCESSFULLY COMPLETED the ETL pipeline")
        print("Pipeline completed SUCCESSFULLY")
    except Exception as ex:
        logging.exception("Error Encountered:", ex)


if __name__ == "__main__":
    inp_year = int(sys.argv[1])
    inp_month = int(sys.argv[2])
    main(inp_year, inp_month)
