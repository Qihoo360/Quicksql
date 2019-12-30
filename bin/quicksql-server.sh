#!/bin/bash
USAGE="-e Usage: quicksql-server.sh {start|stop|restart|status}"
export QSQL_HOME="$(cd "`dirname "$0"`"/..; pwd)"

function start() {
        JAVA_MAIN_CLASS="com.qihoo.qsql.server.JdbcServer"
        QSQL_SERVER_JAR="${QSQL_HOME}/lib/qsql-server-0.6.jar"

        PIDS=`ps -f | grep java | grep "$QSQL_HOME" |awk '{print $2}'`
        if [ -n "$PIDS" ]; then
            echo "ERROR: The $QSQL_HOME quicksql-server already started!"
            echo "PID: $PIDS"
            exit 1
        fi

        #import quicksql.sh related environment variables
        . "${QSQL_HOME}/bin/commons.sh"


        if [[ -n "$SPARK_HOME" ]] && [[ -x "$SPARK_HOME/bin/spark-submit" ]];  then
            SPARK_RUNNER="${SPARK_HOME}/bin/spark-submit"
        else
             if [ `command -v spark-submit` ]; then
                 SPARK_RUNNER="spark-submit"
             else
                 echo "ERROR: SPARK_HOME is not set" >&2
                 exit 1
             fi
        fi

        if [[ "$SPARK_RUNNER" ]]; then
             version=$("$SPARK_RUNNER" --version 2>&1 | awk -F 'version' '/version/ {print $2}')
             IFS=.   read major minor extra <<< "$version";
             if (( major >= 2));
             then
                 if (( minor < 2));
                 then
                     echo "ERROR: Required spark version >= 2.2"
                     exit 1
                 fi
             else
                 echo "ERROR: Required spark version >= 2.2"
                 exit 1
             fi
        fi

        QSQL_JARS=${QSQL_HOME}/lib/sqlite-jdbc-3.20.0.jar,${QSQL_HOME}/lib/qsql-meta-0.6.jar,${QSQL_HOME}/lib/jackson-dataformat-cbor-2.8.10.jar,${QSQL_HOME}/lib/jackson-dataformat-smile-2.8.10.jar,${QSQL_HOME}/lib/qsql-client-0.6.jar,${QSQL_HOME}/lib/jetty-http-9.2.19.v20160908.jar,${QSQL_HOME}/lib/jetty-io-9.2.19.v20160908.jar,${QSQL_HOME}/lib/jetty-security-9.2.19.v20160908.jar,${QSQL_HOME}/lib/jetty-server-9.2.19.v20160908.jar,${QSQL_HOME}/lib/jetty-util-9.2.19.v20160908.jar,${QSQL_HOME}/lib/commons-cli-1.3.1.jar,${QSQL_HOME}/lib/avatica-core-1.12.0.jar,${QSQL_HOME}/lib/avatica-server-1.12.0.jar,${QSQL_HOME}/lib/avatica-metrics-1.12.0.jar,${QSQL_HOME}/lib/protobuf-java-3.3.0.jar,${QSQL_HOME}/lib/jackson-core-2.6.5.jar,${QSQL_HOME}/lib/jackson-annotations-2.6.5.jar,${QSQL_HOME}/lib/jackson-databind-2.6.5.jar,${QSQL_HOME}/lib/httpclient-4.5.6.jar,${QSQL_HOME}/lib/httpcore-4.4.10.jar,${QSQL_HOME}/lib/esri-geometry-api-2.2.0.jar,${QSQL_HOME}/lib/guava-19.0.jar,${QSQL_HOME}/lib/calcite-linq4j-1.17.0.jar,${QSQL_HOME}/lib/derby-10.10.2.0.jar,${QSQL_HOME}/lib/jackson-dataformat-yaml-2.6.5.jar,${QSQL_HOME}/lib/imc-0.2.jar,${QSQL_HOME}/lib/qsql-core-0.6.jar,${QSQL_HOME}/lib/qsql-calcite-analysis-0.6.jar,${QSQL_HOME}/lib/qsql-calcite-elasticsearch-0.6.jar,${QSQL_HOME}/lib/elasticsearch-rest-client-6.2.4.jar,${QSQL_HOME}/lib/httpasyncclient-4.1.2.jar,${QSQL_HOME}/lib/httpclient-4.5.6.jar,${QSQL_HOME}/lib/httpcore-4.4.10.jar,${QSQL_HOME}/lib/httpcore-nio-4.4.5.jar,${QSQL_HOME}/lib/mysql-connector-java-5.1.20.jar,${QSQL_HOME}/lib/elasticsearch-spark-20_2.11-6.2.4.jar
        QSQL_LAUNCH_CLASSPATH="${QSQL_JARS}"

        LOGS_DIR=""
        if [ -n "$LOGS_FILE" ]; then
            LOGS_DIR=`dirname $LOGS_FILE`
        else
            LOGS_DIR=$QSQL_HOME/logs
        fi
        if [ ! -d $LOGS_DIR ]; then
            mkdir $LOGS_DIR
        fi
        STDOUT_FILE=$LOGS_DIR/stdout.log

        PORT=""
        if [ -n "$1" ]; then
            PORT="--port $1"
        fi

        nohup ${SPARK_RUNNER} --jars "${QSQL_LAUNCH_CLASSPATH}" --master "${2}" --executor-memory 1G --driver-memory 3G --num-executors 20 --conf spark.driver.userClassPathFirst=true --class  ${JAVA_MAIN_CLASS}  ${QSQL_SERVER_JAR} --port "${3}"  > $STDOUT_FILE 2>&1 &

        COUNT=0
        while [ $COUNT -lt 1 ]; do
            echo -e ".\c"
            sleep 1
            COUNT=`ps -f | grep java | grep "$QSQL_HOME" | awk '{print $2}' | wc -l`
            if [ $COUNT -gt 0 ]; then
                break
            fi
        done
        echo "OK!"
        PIDS=`ps -f | grep java | grep "$QSQL_HOME" | awk '{print $2}'`
        echo "PID: $PIDS"
        echo "STDOUT: $STDOUT_FILE"
}

function stop() {
        PIDS=`ps -ef | grep java | grep "$QSQL_HOME" |awk '{print $2}'`
        if [ -z "$PIDS" ]; then
            echo "ERROR: The $QSQL_HOME quicksql-server does not started!"
            exit 1
        fi
        echo -e "Stopping the $QSQL_HOME quicksql-server ...\c"
              for PID in $PIDS ; do
                  kill $PID > /dev/null 2>&1
              done

              COUNT=0
              while [ $COUNT -lt 1 ]; do
                  echo -e ".\c"
                  sleep 1
                  COUNT=1
                  for PID in $PIDS ; do
                      PID_EXIST=`ps -f -p $PID | grep java`
                      if [ -n "$PID_EXIST" ]; then
                        COUNT=0
                          break
                      fi
                  done
              done

              echo "OK!"
              echo "PID: $PIDS"
}

function find_app_process() {
        PIDS=`ps -ef | grep java | grep "$QSQL_HOME" |awk '{print $2}'`
        if [ -z "$PIDS" ]; then
            echo "The $QSQL_HOME quicksql-server does not running!"
            exit 1
        else
                echo "The $QSQL_HOME quicksql-server is running!"
                exit 1
        fi
}

runner="spark"
master="local[1]"
port="5888"

case "${1}" in
    start)
        shift
        while getopts :m:p:r: OPTION;
        do
            case $OPTION in
                m) master=$OPTARG
                ;;
                p) port=$OPTARG
                ;;
                r) runner=$OPTARG
                ;;
                *)echo "no option -$OPTARG"
                exit 1
                ;;
            esac
        done
        start ${runner} ${master} ${port}
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        shift
            while getopts :m:p:r: OPTION;
            do
                case $OPTION in
                    m) master=$OPTARG
                    ;;
                    p) port=$OPTARG
                    ;;
                    r) runner=$OPTARG
                    ;;
                    *)echo "no option -$OPTARG"
                    exit 1
                    ;;
                esac
            done
        start ${runner} ${master} ${port}
        ;;
    status)
        find_app_process
        ;;
    *)
    echo ${USAGE}
esac

