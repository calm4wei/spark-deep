#!/bin/sh

BIN_DIR=$(cd $(dirname $0); pwd)
echo $BIN_DIR

SPARK_SUBMIT="/opt/toolbar/spark-1.6.0-bin-hadoop2.6/bin/spark-submit"

echo $SPARK_SUBMIT
$SPARK_SUBMIT \
  --class cn.cstor.face.BatchCompare \
  --master yarn \
  --deploy-mode client \
  --executor-memory 30G \
  --executor-cores 20 \
  --properties-file $BIN_DIR/conf/cstor-spark.properties \
  cstor-deep-1.0-SNAPSHOT.jar
