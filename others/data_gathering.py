
import boto3
from botocore.exceptions import NoCredentialsError




def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


uploaded = upload_to_aws('/Users/smiley/Documents/Insight/Insight_Data_Engineering_Project/Data/RC_2005-12.bz2',
                         'reddit-comments-raw', 's3_reddit_2015_comments')


# read parquet
import findspark
findspark.init()

# load spark_packages
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

# spark context
sc=SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "XXXX")
sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "XXX")

# SparkSQL context
sqlContext = SQLContext(sc)

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