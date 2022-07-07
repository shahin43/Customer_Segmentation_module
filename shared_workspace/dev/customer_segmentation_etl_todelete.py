import numpy as np 
import pandas as pd
import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import expr
from pyspark.sql.window import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.window as Window
from transformer import fixna, cleansing, etl_preprocessing
from pyspark.sql.window import Window
import json
import sys
import argparse
import logging 



def fetchData(s3path):
    # df = spark.read.parquet('s3a://dh-data-chef-hiring-test/data-eng/voucher-selector/data.parquet.gzip')  
    logger.info(f'Reading data from {s3path}')
    df = spark.read.parquet(s3path )
    ## perform transformations on the df 
    df = cleansing(df)
    df = fixna(df)
    logger.info(df.printSchema())
    return df


def countryfilter(df, countrycode):
    logger.info(f'Country wise - voucher data ')
    logger.info(df.groupBy('country_code').agg(count('*').alias('vouchers')).show())
    logger.info(f'filtering data for country : {countrycode}')
    df_filtered = df.filter(col("country_code") == countrycode)
    return df_filtered


def save_customer_segment_data(df_receny, df_frequent, country_code):
    results_frequent_segment = df_frequent.toJSON().map(lambda j: json.loads(j)).collect()
    results_recency_segment = df_receny.toJSON().map(lambda j: json.loads(j)).collect()

    customer_segment = {}
    customer_segment['frequent_segment'] = {}
    customer_segment['recency_segment'] = {}

    for res1 in results_frequent_segment:
        customer_segment['frequent_segment'][res1.get('frequent_segment')] = res1.get('voucher_amount')

    for res2 in results_recency_segment:
        customer_segment['recency_segment'][res2.get('recency_segment')] = res2.get('voucher_amount')

    filename = f'/opt/workspace/data/{country_code}_customer_segment_data.json'
    with open(filename, 'w') as fp:
         json.dump(customer_segment, fp)
    logger.info(f'saving segments data ==> {filename}')
    logger.info('Customer segmentation job - completed  !!')






def main(country_code, s3path):
    df_customer_voucher = fetchData(s3path)

    ## filter for required country
    df_cc = countryfilter(df_customer_voucher, country_code)
    logger.info(df_cc.count() )

    ## performing preprocessing for customer segmentation
    df_cc_preprocessed, df_recency_segment, df_frequent_segment = etl_preprocessing(df_cc, logger)
    
    ## filtered df counts for segments
    print(df_cc_preprocessed.count(), df_recency_segment.count(), df_frequent_segment.count())
    
    ## save segment data in json format; to be consumed by api
    save_customer_segment_data(df_recency_segment, df_frequent_segment, country_code)


def setup_logging():
    """
    Initial setup of logging
    """
    logger_name = 'SPARK:ETL-PROCESS: '
    recipesLogger = logging.getLogger('CUSTOMER-SEGMENTATION:LOGGER') 
    recipesLogger.setLevel(logging.INFO)
    format = '%(asctime)s %(name)-15s %(threadName)-15s %(levelname)-7s ' + logger_name + ' %(message)s'
    #handler = logging.StreamHandler(sys.stderr)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(format))
    recipesLogger.addHandler(handler)
    recipesLogger.info('---- Start Customer segmentation data processing ----')
    return recipesLogger



def get_options(parser, args):
    parsed, extra = parser.parse_known_args(args[1:])
    #print("Found arguments:", vars(parsed))
    if extra:
        logger.error('Found unrecognized arguments:', extra)
    return vars(parsed)
       

if __name__ == "__main__":
    ## set spark config parameters
    spark = SparkSession.builder.master("local").appName("read_file"). \
                                config("spark.driver.extraClassPath","/jars/hadoop-common-3.2.0.jar: " \
                                                                     "jars/aws-java-sdk-1.11.30.jar:/ "\
                                                                     "jars/hadoop-aws-3.2.0.jar"). \
                                getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
                                  
    ## enabling s3a jars to read from s3 path 
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")


    ## arg parser taking params 
    parser = argparse.ArgumentParser(description='Spark pipeline - Customer segmentation !!')
    parser.add_argument('--country', required=False, help='Provide country code for customer segmentation')
    parser.add_argument('--s3path'   , required=False, help='Provide s3 file path for data - iinput:s3_file')
    
    args = sys.argv
    options = get_options(parser, args)
    country =  options['country']
    s3path = options['s3path']
    global logger 
    logger = setup_logging()
    main(country, s3path)