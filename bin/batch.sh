#!/bin/sh

SPARK_SUBMIT="/home/hadoop/spark/bin/spark-submit"

Usage="Usage: \n
$0 <c> \n
c\tETLData, clear the reduplicative data."

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
  c)
    CLASS="cn.cstor.face.ETLData"
    CONF="$ROOT/conf/cstor-spark-batch.properties"
    LOG_FILE="ETLData.out"
    PID_FILE="ETLData.pid"
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
