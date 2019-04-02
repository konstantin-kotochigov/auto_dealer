export HADOOP_USER_NAME=hdfs
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
PYSPARK_PYTHON=./venv/bin/python spark2-submit --master yarn --deploy-mode cluster \
 --conf "spark.pyspark.virtualenv.enabled=true" \
 --conf "spark.pyspark.virtualenv.type=native" \
 --conf "spark.pyspark.virtualenv.bin.path=./venv/bin" \
 --conf "spark.pyspark.python=./venv/bin/python" \
 --jars "spark-avro_2.11-3.2.0.jar" \
 --archives venv.zip#venv \
 --conf "spark.executor.memory=10g" \
 --conf "spark.driver.memory=20g" \
 --conf "spark.yarn.executor.memoryOverhead=10g" \
 --conf "spark.kryoserializer.buffer.max=2047m" \
 --conf "spark.driver.maxResultSize=10g" \
 --conf "spark.driver.cores=4" \
 --files schema.avsc \
 --py-files "cj_loader.py,cj_predictor.py,cj_export.py" \
 --name "analytic_attributes" \
 main.py nosend refit 0.25
