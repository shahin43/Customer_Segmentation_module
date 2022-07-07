import sys
import json
from voluptuous import *
from voluptuous.humanize import *
import datetime
from classes.validator import Validator
import logging


def handler(event, context):
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully! Powered by serverless  !!!!!",
        "input": event
    }


    print(f'payload : {event}')
    validator = Validator()
    ## check if payload passed is valid
    validationcheck = validator.validateSchema(event)
    if validationcheck != 'Pass':
       return validationcheck 

    ##check if dates valid
    validDateCheck = validator.validateDates(event)
    if validDateCheck != 'Pass':
       return validDateCheck

    ##check if dates ranges for first_order_ts & last_order_ts
    validDateRangeCheck = validator.validateDateRange(event)
    if validDateRangeCheck != 'Pass':
       return validDateRangeCheck

    ##check if valid country code
    validCountryCheck= validator.validateCountryCode(event)
    if validCountryCheck != 'Pass':
       return validCountryCheck

    ##check if valid customer segment
    validSegment= validator.validateSegments(event)
    if validSegment != 'Pass':
       return validSegment

    if event['segment_name'] == 'recency_segment':
       voucher_val = fetch_recency_segment_voucher(event)
    else :
        voucher_val = fetch_frequent_segment_voucher(event)

    res = {"voucher amount" : voucher_val }
    
    response = {
        "statusCode": 200,
        "body": res
    }


    return response


def fetch_recency_segment_voucher(event):
   print(f"Fetching voucher amount for : {event['segment_name']}")

   ## get days difference b/w last_order and current_date of processing
   last_order_ts = datetime.datetime.strptime(event['last_order_ts'], '%Y-%m-%d %H:%M:%S')
   first_order_ts = datetime.datetime.strptime(event['first_order_ts'], '%Y-%m-%d %H:%M:%S')
   current_ts = datetime.datetime.today()
   days_difference = (current_ts - last_order_ts).days
   print(f'Days difference : {days_difference}')

   ## bucket for recency_segments 
   bucket = { range(0, 30):  '0-30',
              range(30, 60):  '30-60',
              range(60, 90):  '60-90',
              range(90, 120): '90-120',
              range(120,180): '120-180',
              range(180, 10000): '180+'
            }

   segment_val = [val for key, val in bucket.items() if days_difference in key][0]

   print(f'Segment value is : {segment_val}')
   filename = './data/{country_code}_customer_segment_data.json'.format(country_code=event['country_code'])
   
   return fetch_vouchervalue(event['segment_name'], segment_val, filename) 





def fetch_frequent_segment_voucher(event):
   print(f"Fetching voucher amount for : {event['segment_name']}")
   
   ## bucket for frequent_segments 
   bucket = { range(0, 5):  '0-4',
              range(5, 14):  '5-13',
              range(14, 38):  '13-37',
              range(30, 10000): '37+'
            }

   segment_val = [val for key, val in bucket.items() if event['total_orders'] in key][0]

   print(f"Order value is : {event['total_orders']}")
   print(f'Segment value is : {segment_val}')
   filename = './data/{country_code}_customer_segment_data.json'.format(country_code=event['country_code'])

   return fetch_vouchervalue(event['segment_name'], segment_val, filename) 



def fetch_vouchervalue(segment_name, segment_value, filename):
   f = open(filename,)
   data = json.load(f) 
   
   ## fetch voucher value from segmentation model data 
   voucher_value = data.get(segment_name,None).get(segment_value,'Not available')
   print(f"Voucher value for :- {segment_name}-{segment_value} is {voucher_value} ")
   return voucher_value





################################################################################################################
################################################################################################################

##curl -XPOST "http://localhost:8080/2015-03-31/functions/function/invocations" -d '{"payload":"hello ShahinAli !"}' 
## docker run --rm -p 9000:8080 aws-lambda-python:latest  



## input 
# { 
# "customer_id": 123,   
# "country_code": "Peru",  
# "last_order_ts": "2018-05-03 00:00:00", 
# "first_order_ts": "2017-05-03 00:00:00", 
# "total_orders": 15,  
# "segment_name": "recency_segment" 
# }