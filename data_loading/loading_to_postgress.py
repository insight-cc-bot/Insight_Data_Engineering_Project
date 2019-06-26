import findspark
findspark.init()

# load spark_packages
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

# spark context
sc=SparkContext()
#sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "XXXX")
#sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "XXX")

# SparkSQL context
sqlContext = SQLContext(sc)

filename = "RC_2006-01.parquet"
destination="s3a://reddit-comments-raw/2006/comments_2006_05.parquet"
data = sqlContext.read.parquet(destination)
print("done",data.count())

from pyspark.sql import DataFrameWriter
my_writer = DataFrameWriter(data)


md="overwrite"
database_name = 'reddit'
hostname = 'ec2-54-214-117-182.us-west-2.compute.amazonaws.com'
url_connect = "jdbc:postgresql://{hostname}:5000/{db}".format(hostname=hostname, db=database_name)
properties = {"user":"webuiuser","password" : "webuiuser","driver": "org.postgresql.Driver"}
table="word_count"

my_writer.jdbc(url=url_connect,table= table,mode=md,properties=properties)


