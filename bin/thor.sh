#!/bin/sh

if [ x"$1" = x ]; then
    echo "Usage : thor.sh [worker.properties] [connect.properties ...]"
    exit 1
fi

jdk8=$(ls /usr/java/ | grep 1.8.0)
if [ "x$jdk8" = "x" ]; then
    echo "Java version must be 1.8.0 or later! current version is "$JAVA_VERSION
    exit
fi

jdk=""
for dir in $jdk8;
do
  if [[ $dir > $jdk ]]; then
    jdk=$dir
  fi
done

export JAVA_HOME=/usr/java/$jdk
export CLASSPATH=.:$JAVA_HOME/lib:$JAVA_HOME/lib/tools.jar:$JAVA_HOME/lib/dt.jar
export PATH=$JAVA_HOME/bin:$PATH

# JAVA_VERSION=$(java -version 2>&1 | awk 'NR==1{ gsub(/"/,""); print $3 }i' | awk -F '_' '{print $1}')

# if [[ "x$JAVA_VERSION" < "x1.8.0" ]]; then
#     echo "Java version must be 1.8.0 or later! current version is "$JAVA_VERSION
#     exit
# fi

base_dir=$(dirname $0)

if [ "x$THOR_LOG4J_OPTS" = "x" ]; then
    export THOR_LOG4J_OPTS="-Dlog4j.configurationFile=file:$base_dir/../config/log4j2.xml"
fi

if [ "x$THOR_HEAP_OPTS" = "x" ]; then
    export THOR_HEAP_OPTS="-Xmx1G -Xms1G"
fi

# EXTRA_ARGS="-name thor -loggc"

offset_line=$(sed -n '/^offset.storage.file.filename=/p' $1)
offset_file=${offset_line#*=}

if [ x"$offset_file" = x ]; then
    echo $1" 中必须配置存储 offset 本地文件，例如：offset.storage.file.filename=/tmp/connect.offset"
    exit 1
fi

offset_dir=${offset_file%/*}

if [ ${offset_dir:0:1} != "/" ]; then
    offset_dir=$base_dir"/"$offset_dir
fi

if [ ! -d $offset_dir ]; then
    mkdir -p $offset_dir
fi

exec $(dirname $0)/run-class.sh $EXTRA_ARGS org.apache.kafka.connect.cli.ConnectStandalone "$@"
