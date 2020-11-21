export SPARK_MAJOR_VERSION=2
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8G \
  --executor-memory 8G \
  --executor-cores 8 \
  --num-executors 8 \
  --py-files /home/hdfs/spark_scripts/Autorizzativo_json_to_hive_visa.py \
  /home/hdfs/spark_scripts/Autorizzativo_json_to_hive_visa.py &> /home/hdfs/spark_scripts/log_spark.txt

