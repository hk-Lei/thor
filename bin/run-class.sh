#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-daemon] [-name servicename] [-loggc] classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/..

# classpath addition for release
CLASSPATH=$CLASSPATH:$base_dir/libs/*

# Log directory to use
if [ "x$LOG_DIR" = "x" ]; then
    LOG_DIR="$base_dir/logs"
fi

# create logs directory
if [ ! -d "$LOG_DIR" ]; then
  mkdir -p "$LOG_DIR"
fi

THOR_LOG4J_OPTS="-Dkafka.logs.dir=$LOG_DIR $THOR_LOG4J_OPTS"

# Generic jvm settings you want to add
if [ -z "$THOR_OPTS" ]; then
  THOR_OPTS=""
fi

# Set Debug options if enabled
if [ "x$THOR_DEBUG" != "x" ]; then

    # Use default ports
    DEFAULT_JAVA_DEBUG_PORT="5005"

    if [ -z "$JAVA_DEBUG_PORT" ]; then
        JAVA_DEBUG_PORT="$DEFAULT_JAVA_DEBUG_PORT"
    fi

    # Use the defaults if JAVA_DEBUG_OPTS was not set
    DEFAULT_JAVA_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=${DEBUG_SUSPEND_FLAG:-n},address=$JAVA_DEBUG_PORT"
    if [ -z "$JAVA_DEBUG_OPTS" ]; then
        JAVA_DEBUG_OPTS="$DEFAULT_JAVA_DEBUG_OPTS"
    fi

    echo "Enabling Java debug options: $JAVA_DEBUG_OPTS"
    THOR_OPTS="$JAVA_DEBUG_OPTS $THOR_OPTS"
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Memory options
if [ -z "$THOR_HEAP_OPTS" ]; then
  THOR_HEAP_OPTS="-Xmx256M"
fi

# JVM performance options
if [ -z "$THOR_JVM_PERFORMANCE_OPTS" ]; then
  THOR_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi


while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -loggc)
      if [ -z "$THOR_GC_LOG_OPTS" ]; then
        GC_LOG_ENABLED="true"
      fi
      shift
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

# GC options
GC_FILE_SUFFIX='-gc.log'
GC_LOG_FILE_NAME=''
if [ "x$GC_LOG_ENABLED" = "xtrue" ]; then
  GC_LOG_FILE_NAME=$DAEMON_NAME$GC_FILE_SUFFIX
  THOR_GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps "
fi

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  nohup $JAVA $THOR_HEAP_OPTS $THOR_JVM_PERFORMANCE_OPTS $THOR_GC_LOG_OPTS $THOR_JMX_OPTS $THOR_LOG4J_OPTS -cp $CLASSPATH $THOR_OPTS "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
else
  exec $JAVA $THOR_HEAP_OPTS $THOR_JVM_PERFORMANCE_OPTS $THOR_GC_LOG_OPTS $THOR_JMX_OPTS $THOR_LOG4J_OPTS -cp $CLASSPATH $THOR_OPTS "$@"
fi
