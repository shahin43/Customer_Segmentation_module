

# Customer Segmentation & Voucher Selection API - Data Engineering project

The POC project can be categorized into two parts : 
  - ETL batch process; pipeline for processing customer segmentation data (ETL batch process, using Spark)
  - Voucher selection API  (data exposed for consuming, using AWS lambda, API gateway)

Entire solution has been containarized using docker, by launching a spark cluster and AWS lambda container, build and deployed locally for completing the POC using docker. 

## Solution Overview  

- ETL batch process
  ETL pipeline is developed using Pyspark. Entire solution have been packaged into containarized solution using docker, by launching a spark cluster locally (spinning up 1-master, 1-executor and 1-sparkclient for submiting the application to cluster on docker). Docker-compose is used to spin up containers, defining dependencies and enabling the cluster for running spark application. Once cluster is up, Pyspark job - `customer_segmentation_etl.py` is submitted to run on cluster with respective parameters passed using spark-submit, trigerred from spark-client container. 

_Images added here are those which I've been using for development purpose of spark 3.0 on kubernetes._

Below are the environment details : 
 - Python 3.7, PySpark 3.0.2, Java 8 
 - Apache Spark 3.0.2 with one master and two worker nodes
 - HDFS 3.2. shipped with the image
 - AWS lambda-python base image : public.ecr.aws/lambda/python:3.8

### Building the Spark cluster

Steps :

  Pull the branch changes and build the images using below command
  
 ``` 
    sh buildCluster.sh 
 ``` 
  Above command would build the respective images for `spark master`, `worker node`, `client` based on base images and `aws-python-lambda` image using base images.

  Once images are build, launch containers using `docker-compose.yml` which spins up the cluster and aws-lambda containers.  

 ``` 
   docker-compose up -d 
 ``` 
 

### Executing the spark job on cluster
Local spark cluster is available now, pyspark script for customer segmentation etl jobs can be now deployed and executed on the cluster as below from spark-client: 
  
```
docker exec -it spark-client bash -c '$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --num-executors 2 --name spark-cluster-job --conf spark.executor.instances=2 --conf spark.executor.memory=512m /opt/workspace/etl/customer_segmentation_etl.py --country Peru --s3path s3a://dh-data-chef-hiring-test/data-eng/voucher-selector/data.parquet.gzip'
```

_Monitor the cluster here : http://localhost:8080/_



### PySpark job customer_segmentation_etl.py - overview/assumptions

The job reads data from s3 path provided, using respective jars configured _(spark config is added with required jar files for reading from s3, passing required IAM credentials for aws user)_. The script takes input parameters for `s3filepath` and `country code` as below : 

Parameters to be passed for the job to execute are : 

      - `s3path  :- s3_file path`  
      - `country :- country code, for which customer segmentation to be generated` 

Below are few key tasks performed in the job:
 - `Data cleaning and transformation` : Fixed the schema of data for timestamp, double fields. Fixed null, missing data points. There were few data issues identified in data set, like `event_timestamp` in certain cases are earlier than `first_order_ts` for customers, which was assumed as bug in data or some sort of data issue. Hence days_difference calculated between `last_order_ts` & `event_timstamp` is negative value in these cases. Recency_segment defined for these events would be `Unknown`. _Data analysis prior to preparation was done using jupyter notebook, `Customer churn analysis.ipynb` (added along with files)._
    
 - Days differnce is calculated between event_timestamp and last_order_ts, inorder to define customer segmentation based on recency_segment and customer orders data is considered for defining frequent_segment for requried segment values provided. Maximum count (events) of vouchers against each segment values are considered as `voucher value`, which feeds the API. 

 - Once defining the customer segmentation and voucher value data is processed for each segments, output data is dumped into a json file in shared volume path within containers:`/opt/workspace/data/` folder, with file name format  `<country_code>_customer_segment_data.json`. The output file would be accessible with the api script to retrieve corresponding voucher values for payloads passed in the POST method. In production, this can be saved either in a NOSQL db like Dynamodb or even maintined in separate s3 location based how api is going to be invoked. 
 
 _Assuming that in production s3 data would be available as partitions, example if partitioned by country, would be more efficient rather than single s3 location having data for whole countries._


### Voucher selection API -  overview/assumptions

API is containarized using docker base image from aws lambda, container is launched by docker-compose. Container `aws-python-lambda-api` would be available exposing port 9000 on the host which can be curl'ed with payload for retrieveing voucher amount data as response. In certain cases, segment values are not available, where api return `Not available` for corresponding segments. 

`app.py` script is reading from json output file generated by the customer_segmentation ETL batch job, which would be available under shared path in the container at `/opt/workspace/data/`. Unit tests (`test_app.py`) and schema/ data validations are added to the api script, for handling most of the errors.


#### Run below curl command for retrieving api output, as container ports are mapped on to host port-9000 (kindly check if 9000 port is already in use on host, if any error)

```
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{ "customer_id": 123,   "country_code": "Peru", "first_order_ts": "2017-05-03 00:00:00", "total_orders": 20,  "segment_name": "frequent_segment",  "last_order_ts": "2021-04-04 00:00:00" }'
```


#### Production deployment 
- Above pipeline is setup for POC, in a production environment pyspark job can be either trigerred using AWS Glue, EMR or Spark-cluster on Kubernetes.
- Images used here can be pushed to ECR and used to setup cluster in production, instead of using local versions.
- Lambda/ API gateway resources should be deployed in respective AWS regions using Serverless framweork/terraforms, instead of docker image in production. 



## Tests scripts

```
-- API Invoke 
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{ "customer_id": 123,   "country_code": "Peru", "first_order_ts": "2017-05-03 00:00:00", "total_orders": 20,  "segment_name": "frequent_segment",  "last_order_ts": "2021-04-04 00:00:00" }'


docker exec -it spark-client bash -c '$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --num-executors 2 --name spark-cluster-job --conf spark.executor.instances=2 --conf spark.executor.memory=512m /opt/workspace/customer_segmentation_etl.py --country Peru --s3path s3a://dh-data-chef-hiring-test/data-eng/voucher-selector/data.parquet.gzip'

```
