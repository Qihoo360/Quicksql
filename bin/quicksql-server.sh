#!/bin/bash
USAGE="-e Usage: quicksql-server.sh {start|stop|restart|status}"
export QSQL_HOME="$(cd "`dirname "$0"`"/..; pwd)"

function start() {
        #import quicksql.sh related environment variables
        . "${QSQL_HOME}/bin/commons.sh"

        JAVA_MAIN_CLASS="com.qihoo.qsql.server.JdbcServer"
        QSQL_SERVER_JAR="${QSQL_HOME}/lib/qsql-server-"${PROJECT_VERSION}".jar"

        PIDS=`ps -f | grep java | grep "$QSQL_HOME" |awk '{print $2}'`
        if [ -n "$PIDS" ]; then
            echo "ERROR: The $QSQL_HOME quicksql-server already started!"
            echo "PID: $PIDS"
            exit 1
        fi

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

        if [ "${1}" = "flink" ] ; then
           if [[ -n "$FLINK_HOME" ]] && [[ -x "$FLINK_HOME/bin/flink" ]];  then
              FLINK_RUNNER="${FLINK_HOME}/bin/flink run"
           else
              if [ `command -v flink` ]; then
                 FLINK_RUNNER="flink run"
              else
                echo "ERROR: FLINK_HOME is not set" >&2
                exit 1
            fi
           fi
           buildFatJar
           #CONF=${CONF}" --jar_name=${QSQL_HOME}/lib/server/qsql-server-"${PROJECT_VERSION}".jar "
           QSQL_SERVER_JAR="${QSQL_HOME}/lib/server/qsql-server-"${PROJECT_VERSION}-fat".jar"
           nohup ${FLINK_RUNNER} -c ${JAVA_MAIN_CLASS} ${QSQL_SERVER_JAR} --port "${3}"  > $STDOUT_FILE 2>&1 &
           echo " nohup ${FLINK_RUNNER} -c ${JAVA_MAIN_CLASS} ${QSQL_SERVER_JAR} --port "${3}"  > $STDOUT_FILE 2>&1 & "
        else
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

            QSQL_LAUNCH_CLASSPATH=${QSQL_HOME}/lib/sqlite-jdbc-*.jar,${QSQL_HOME}/lib/qsql-meta-"${PROJECT_VERSION}".jar,${QSQL_HOME}/lib/jackson-dataformat-cbor-*.jar,${QSQL_HOME}/lib/jackson-dataformat-smile-*.jar,${QSQL_HOME}/lib/qsql-client-"${PROJECT_VERSION}".jar,${QSQL_HOME}/lib/jetty-http-*.jar,${QSQL_HOME}/lib/jetty-io-*.jar,${QSQL_HOME}/lib/jetty-security-*.jar,${QSQL_HOME}/lib/jetty-server-*.jar,${QSQL_HOME}/lib/jetty-util-*.jar,${QSQL_HOME}/lib/commons-cli-*.jar,${QSQL_HOME}/lib/avatica-core-*.jar,${QSQL_HOME}/lib/avatica-server-*.jar,${QSQL_HOME}/lib/avatica-metrics-*.jar,${QSQL_HOME}/lib/protobuf-java-*.jar,${QSQL_HOME}/lib/jackson-core-*.jar,${QSQL_HOME}/lib/jackson-annotations-*.jar,${QSQL_HOME}/lib/jackson-databind-*.jar,${QSQL_HOME}/lib/httpclient-*.jar,${QSQL_HOME}/lib/httpcore-*.jar,${QSQL_HOME}/lib/esri-geometry-api-*.jar,${QSQL_HOME}/lib/guava-*.jar,${QSQL_HOME}/lib/derby-*.jar,${QSQL_HOME}/lib/jackson-dataformat-yaml-*.jar,${QSQL_HOME}/lib/imc-*.jar,${QSQL_HOME}/lib/qsql-core-"${PROJECT_VERSION}".jar,${QSQL_HOME}/lib/qsql-calcite-parser-"${PROJECT_VERSION}".jar,${QSQL_HOME}/lib/elasticsearch-rest-client-*.jar,${QSQL_HOME}/lib/httpasyncclient-*.jar,${QSQL_HOME}/lib/httpclient-*.jar,${QSQL_HOME}/lib/httpcore-*.jar,${QSQL_HOME}/lib/httpcore-nio-*.jar,${QSQL_HOME}/lib/mysql-connector-java-*.jar,${QSQL_HOME}/lib/elasticsearch-spark-*.jar,${QSQL_HOME}/lib/ojdbc6-*.jar,${QSQL_HOME}/lib/json-path-*.jar

            echo "nohup ${SPARK_RUNNER} --jars "${QSQL_LAUNCH_CLASSPATH}" --master "${2}" --executor-memory 1G --driver-memory 3G --num-executors 20 --conf spark.driver.userClassPathFirst=true --class  ${JAVA_MAIN_CLASS}  ${QSQL_SERVER_JAR} --port "${3}"  > $STDOUT_FILE 2>&1 &"
            nohup ${SPARK_RUNNER} --jars "${QSQL_LAUNCH_CLASSPATH}" --master "${2}" --executor-memory 1G --driver-memory 3G --num-executors 20 --conf spark.driver.userClassPathFirst=true --class  ${JAVA_MAIN_CLASS}  ${QSQL_SERVER_JAR} --port "${3}"  > $STDOUT_FILE 2>&1 &
        fi
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

function buildFatJar(){
  JAR_FILE="${QSQL_HOME}/lib/server/qsql-server-"${PROJECT_VERSION}"-fat.jar"

 if [ ! -f "${JAR_FILE}" ]; then
    echo "INFO: Start build server fat jar...";
    mkdir "${QSQL_HOME}/lib/server/tmp"

    cp "${QSQL_HOME}"/lib/server/*.jar "${QSQL_HOME}"/lib/server/tmp/

    cd "${QSQL_HOME}"/lib/server/tmp

    for jarFile in $(ls *.jar | grep -v "qsql-server-${PROJECT_VERSION}.jar"); do
        jar -xf "${jarFile}"
        echo "${jarFile}"
    done
    rm -rf META-INF

    jar -xf qsql-server-"${PROJECT_VERSION}".jar
    if [ $? -ne 0 ]; then
       echo "ERROR: File qsql-server-"${PROJECT_VERSION}".jar is not exist.";
       exit 1
    fi

    ls |grep -v jar$ | xargs jar cfM qsql-server-"${PROJECT_VERSION}-fat".jar
    if [ $? -ne 0 ]; then
       echo "ERROR: Failed buid server fat jar qsql-server-"${PROJECT_VERSION}-fat".jar.";
       exit 1
    fi

    mv qsql-server-"${PROJECT_VERSION}"-fat.jar "${QSQL_HOME}"/lib/server
    cd ../../
    rm -rf "${QSQL_HOME}"/lib/server/tmp
    echo "INFO: End build server fat jar.";
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

