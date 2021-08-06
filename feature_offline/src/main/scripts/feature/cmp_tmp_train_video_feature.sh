#!/usr/bin/env bash

##-*-coding: utf-8; mode: shell-script;-*-##

set -e
umask 0000

APP_HOME="$(cd $(dirname $0)/../..; pwd -P)"

set -a
source "$APP_HOME/etc/run_online.env"
set +a

echo "run_online.sh:"$*

if [[ $# -eq 1 ]]; then
    dt=$(date -d "$1" +%Y-%m-%d)
    num=0
elif [[ $# -eq 2 ]]; then
    dt=$(date -d "$1" +%Y-%m-%d)
    num=$2
else
    echo "$0 date"
    exit 1
fi

export LOG_FILE=$conffile
export LOG_DATE=$dt

if [ ${conffile:0:1} != "/" ]
then
    conffile="$APP_HOME/conf/$conffile"
fi

logging "base date $dt to process: $conffile"

filelist="$APP_HOME/conf/taichi.json,$APP_HOME/conf/log4j.properties,$APP_HOME/conf/conf/demo.csv,$APP_HOME/conf/conf/feature_fm_bucket.json"

export SPARK_CLASSPATH=$SPARK_CLASSPATH":$APP_HOME/conf:$APP_HOME/etc:$APP_HOME/libs/*"

cls="com.captain.bigdata.taichi.demo.app.VideoFeatureColumnApp"
jar="$APP_HOME/libs/$APP_JAR"

cmd="spark-submit"
cmd="$cmd --conf spark.sql.warehouse.dir=file://$(pwd)/sparksql/warehouse"
cmd="$cmd --conf spark.app.name=feature-offline-$conffile-$dt"
cmd="$cmd --conf spark.yarn.submit.waitAppCompletion=true"
cmd="$cmd --files ${filelist}"
cmd="$cmd --num-executors 30 --executor-cores 5 --executor-memory 15GB"

cmd="$cmd --conf spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
cmd="$cmd --conf spark.executor.extraJavaOptions=-Dfile.encoding=utf-8"

#cmd="$cmd --master local[*]"
cmd="$cmd --master yarn"

cmd="$cmd --deploy-mode client"
#cmd="$cmd --deploy-mode cluster"

cmd="$cmd --conf spark.port.maxRetries=30"
cmd="$cmd --conf spark.speculation=true"
cmd="$cmd --conf spark.default.parallelism=100"
cmd="$cmd --name feature-offline-$conffile-$dt"
cmd="$cmd --class $cls $jar"
cmd="$cmd -d $dt"
cmd="$cmd -n $num"

LOG_FILE=${conffile//\//_}_$dt
logging "command to execute: $cmd"
LOG_DIR=$LOG_HOME/$dt

$cmd

logging 'all OK'

