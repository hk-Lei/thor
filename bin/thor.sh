#!/bin/sh

JAVA_VERSION=$(java -version 2>&1 | awk 'NR==1{ gsub(/"/,""); print $3 }i' | awk -F '_' '{print $1}')

if [[ "x$JAVA_VERSION" < "x1.8.0" ]]; then
    echo "Java version must be 1.8.0 or later!"
    exit
fi

base_dir=$(dirname $0)

if [ "x$THOR_LOG4J_OPTS" = "x" ]; then
    export THOR_LOG4J_OPTS="-Dlog4j.configurationFile=file:$base_dir/../config/log4j2.xml"
fi

if [ "x$THOR_HEAP_OPTS" = "x" ]; then
    export THOR_HEAP_OPTS="-Xmx1G -Xms1G"
fi

EXTRA_ARGS="-name thor -loggc"

exec $(dirname $0)/run-class.sh $EXTRA_ARGS org.apache.kafka.connect.cli.ConnectStandalone "$@"
