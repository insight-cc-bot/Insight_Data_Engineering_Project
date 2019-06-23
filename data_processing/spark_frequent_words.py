import json
import sys
import time
import os
import logging
from config import CLEAN_COMMENTS_BUCKET

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


def get_file_name(year, month):
    filename = "{type}_{year}_{month:02d}.parquet".format(type="comments", year=year, month=month)
    source = "s3a://{bucket}/{year}/{file}".format(bucket=CLEAN_COMMENTS_BUCKET, year=year, file=filename)
    return source


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


def generate_word_count(filename_read_S3):
    # get data -
    logging.info("Step 1: read cleaned file into Dataframe from S3")
    # comments_df1 = sqlContext.read.parquet(filename_read_S3)
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

    print(comments_df6.printSchema())
    print(comments_df6.show(10))
    """
    # --------------------------------------------
    print("Step  18: Generate ND-JSON file for the words")
    # ---------------------------------------------
    # Load the Cleaned data to ElasticSearch
    nd_json_words = comments_df17.rdd.map(lambda x: elastic_search_mapper_word(x)).toDF()

    # save as Text file
    output_freq_words = "{dir}/frequent_words_{year}_{month}.csv".format(dir=OUTPUT_DIRECTORY, year=input_year,
                                                                         month=input_month)
    nd_json_words.saveAsTextFile(output_freq_words)
    """


def main(year, month):
    file_read_S3=get_file_name(year, month)
    try:
        generate_word_count(file_read_S3)
    except Exception as ex:
        print("ERROR -{0}".format(ex))


if __name__ == "__main__":
    input_year = int(sys.argv[1])
    input_month = int(sys.argv[2])
    main(input_year, input_month)
