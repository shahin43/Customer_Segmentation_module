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
from pyspark.sql.window import Window


def fixna(df):
    ## fill NA/ None data for orders and voucher amount
    df = df.fillna({'total_orders':0, 'voucher_amount':0})
    return df


def cleansing(df):
    df = df.withColumn('event_timestamp', col('timestamp').cast('timestamp')) \
           .withColumn('last_order_ts', col('last_order_ts').cast('timestamp')) \
           .withColumn('total_orders', col('total_orders').cast('double'))
    return df 



def etl_preprocessing(df, logger):
    df = df \
                 .withColumn("days_since_last_order", F.datediff(df.timestamp, df.last_order_ts).cast('double'))

    
    df_recency = definecustomer_recency_segment(df, logger)
    df_frequent = definecustomer_frequent_segment(df, logger)
    return df, df_recency, df_frequent




def definecustomer_recency_segment(df, logger):
    ## defining recency_segment 
    df_recency = df.selectExpr("*", \
                             "CASE WHEN days_since_last_order >= 30 AND days_since_last_order < 60 THEN '30-60' " \
                                  "WHEN days_since_last_order >= 60 AND days_since_last_order < 90 THEN '60-90' " \
                                  "WHEN days_since_last_order >= 90 AND days_since_last_order < 120 THEN '90-120' " \
                                  "WHEN days_since_last_order >= 120 AND days_since_last_order < 180 THEN '120-180' " \
                                  "WHEN days_since_last_order >= 180 THEN '180+' " \
                                  "ELSE 'Unknown' " \
                              "END AS  recency_segment")
    
    logger.info(f' recency_segment stats')
    logger.info(df_recency.groupBy('recency_segment').agg(count('*').alias('count_recs') ).show() )

    
    cols = ["recency_segment", "rec_counts"]
    windowSpec = Window.partitionBy("recency_segment").orderBy(col("rec_counts").desc()) 
    df_recency = df_recency.groupBy('recency_segment', 'voucher_amount') \
                         .agg(count('*').alias('rec_counts')) \
                         .select('recency_segment','voucher_amount','rec_counts' )  \
                         .select('*' ,F.row_number().over(windowSpec ).alias('rownum_recency_segment') )  \
                         .orderBy(*cols, ascending=False )
    
    df_recency = df_recency.filter(col("rownum_recency_segment") == 1)
    df_recency.show() 
    return df_recency



def definecustomer_frequent_segment(df, logger):
    ## defining frequent_segment 
    df_frequent = df.selectExpr("*", \
                        "CASE WHEN total_orders >= 0 AND total_orders <= 4 THEN '0-4' " \
                             "WHEN total_orders >= 5 AND total_orders <= 13 THEN '5-13' " \
                             "WHEN total_orders > 13 AND total_orders <= 37 THEN '13-37' " \
                             "ELSE 'Unknown' " \
                         "END AS  frequent_segment")
    
    logger.info(f' frequent_segment stats')
    logger.info( df_frequent.groupBy('frequent_segment').agg(count('*').alias('count_recs') ).show())

    cols = ["frequent_segment", "rec_counts"]
    windowSpec = Window.partitionBy("frequent_segment").orderBy(col("rec_counts").desc()) 
    df_frequent = df_frequent.groupBy('frequent_segment', 'voucher_amount') \
                         .agg(count('*').alias('rec_counts')) \
                         .select('frequent_segment','voucher_amount','rec_counts' )  \
                         .select('*' ,F.row_number().over(windowSpec ).alias('rownum_frequent_segment') )  \
                         .orderBy(*cols, ascending=False )

    df_frequent = df_frequent.filter(col("rownum_frequent_segment") == 1)
    df_frequent.show()                     
    return df_frequent


