#!/bin/sh

SPARK_SUBMIT="/home/hadoop/spark/bin/spark-submit"

Usage="Usage: \n
$0 <b|f|c|h> \n
b\tBitFaceCompare, to compare relativity using bitset.\n
f\tFaceCompare, to compare relativity using 4096.\n
c\tETLData, to clear the features code.\n
h\tBatchPersonInfo, to import the features code to hbase."

run (){
    if [ -f $RUN_PATH/$PID_FILE ]; then
        echo "$RUN_PATH/$PID_FILE already exists."
	echo "Now exiting..."
	exit 1
    fi
    $@ > $LOG_PATH/$LOG_FILE 2>&1 &
    PID=$!
    echo $PID > "$RUN_PATH/$PID_FILE"
    wait $PID
    rm -f $RUN_PATH/$PID_FILE
}

if [ $# -ne 1 ]; then
    echo -e "$Usage"
    exit 3
fi

ROOT=$(cd $(dirname $0); pwd)
echo $ROOT


LOG_PATH=$ROOT/logs
RUN_PATH=$ROOT/run
if [ ! -d $LOG_PATH ]; then
    mkdir -p $LOG_PATH
fi
if [ ! -d $RUN_PATH ]; then
   mkdir -p $RUN_PATH
fi

case $1 in
  b)
    CLASS="cn.cstor.face.BitFaceCompare" 
    CONF="$ROOT/conf/cstor-spark-bit.properties"
    LOG_FILE="bitFaceCompare.out"
    PID_FILE="bitFaceCompare.pid"
    ;;
  f)
    CLASS="cn.cstor.face.FaceCompare" 
    CONF="$ROOT/conf/cstor-spark-bit.properties"
    LOG_FILE="faceCompare.out"
    PID_FILE="faceCompare.pid"
    ;;
  c)
    CLASS="cn.cstor.face.ETLData"
    CONF="$ROOT/conf/cstor-spark-batch.properties"
    LOG_FILE="ETLData.out"
    PID_FILE="ETLData.pid"
    ;;
  h)
    CLASS="cn.cstor.face.BatchPersonInfo"
    CONF="$ROOT/conf/cstor-spark-batch.properties"
    LOG_FILE="BatchPersonInfo.out"
    PID_FILE="BatchPersonInfo.pid"
    ;;
  *)
    echo -e $Usage
    exit 2
    ;;
esac

CMD="$SPARK_SUBMIT \
  --class $CLASS \
  --master local[4] \
  --executor-memory 4G \
  --properties-file $CONF \
  cstor-deep-1.0-SNAPSHOT.jar"
echo -e "$CMD"
run "$CMD" &

#CMD="$SPARK_SUBMIT \
#  --class $CLASS \
#  --master spark://datacube201:7077 \
#  --driver-cores 2 \
#  --driver-memory 10G \
#  --total-executor-cores 80 \
#  --executor-memory 10G \
#  --properties-file $CONF \
#  cstor-deep-1.0-SNAPSHOT.jar"
#echo -e "$CMD"
