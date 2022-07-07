# -- Software Stack Version

SPARK_VERSION="3.0.2"
HADOOP_VERSION="3.2"
JUPYTERLAB_VERSION="2.1.5"

# -- Building the Images

docker build \
  -f dockerImages/sparkcluster_baseImage.Dockerfile \
  -t cluster-base .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -f dockerImages/sparkbase.Dockerfile \
  -t spark-base .

docker build \
  -f dockerImages/spark-master.Dockerfile \
  -t spark-master .

docker build \
  -f dockerImages/spark-worker.Dockerfile \
  -t spark-worker .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  -f dockerImages/jupyterlab.Dockerfile \
  -t jupyterlab .

docker build \
  -f Dockerfile \
  -t aws-python-lambda .



# ./bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --num-executors 2 \
#   --name spark-cluster-job \
#   --conf spark.executor.instances=2 \
#   --conf spark.executor.memory=512m \
#   receipes.py 


# docker exec -it spark-client bash -c '$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --num-executors 2 --name spark-cluster-job2 --conf spark.executor.instances=2 --conf spark.executor.memory=512m receipes.py'