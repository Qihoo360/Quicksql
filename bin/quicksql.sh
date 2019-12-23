#!/bin/bash


export QSQL_HOME="$(cd "`dirname "$0"`"/..; pwd)"

#import quicksql.sh related environment variables
. "${QSQL_HOME}/bin/commons.sh"

#parse args
ARGS=`getopt -o "e:h" -l "runner:,master:,worker_memory:,driver_memory:,worker_num:,file:" -n "quicksql.sh" -- "$@"`

eval set -- "${ARGS}"

while true
do
    case "${1}" in
        -e) 
        shift;
        if [[ -n "${1}" ]] ; then
            SQL=${1}
            shift;
        fi

        ;;
        --runner)
        shift 
        case "${1}" in
            "")
            echo "ERROR: No --runner selected, decide to default runner";
            QSQL_RUNNER="${QSQL_DEFAULT_RUNNER}"
            shift ;  
            ;;
            "spark")
            QSQL_RUNNER="SPARK"
            shift ;
            ;;
            "flink")
            QSQL_RUNNER="FLINK"
            shift ;
            ;;
            "jdbc")
            QSQL_RUNNER="JDBC"
            shift ;
            ;;
            "dynamic")
            QSQL_RUNNER="DYNAMIC"
            shift ;
            ;;
            *)
            echo "ERROR: `--runner` error! please select property runner!"
            exit 1
            ;;
        esac
        ;;
        --master)
        shift ;
        case "${1}" in
            "")
            echo "ERROR: No --master selected, decide to default master";
            QSQL_MASTER="${QSQL_DEFAULT_MASTER}"
            shift  
            ;;
            "cluster")
            QSQL_MASTER="yarn-cluster"
            shift;
            ;;
            "client")
            QSQL_MASTER="yarn-client"
            shift;
            ;;
            "yarn-cluster")
            QSQL_MASTER="yarn-cluster"
            shift;
            ;;
            "yarn-client")
            QSQL_MASTER="yarn-client"
            shift;
            ;;
            local*)
            QSQL_MASTER="local[*]"
            shift;
            ;;
            mesos*)
            QSQL_MASTER=${1}
            shift;
            ;;
            spark*)
            QSQL_MASTER=${1}
            shift;
            ;;
            k8s*)
            QSQL_MASTER=${1}
            shift;
            ;;
            *)
            echo "ERROR: `--master` error! please select property master!"
            exit 1
            ;;
        esac
        ;;
        --worker_memory)
        shift ;
        case "${1}" in
            "")
            echo "ERROR: No --worker_memory selected, decide to default worker_memory";
            QSQL_WORKER_MEMORY="${QSQL_DEFAULT_WORKER_MEMORY}"
            shift  
            ;;
            *)
            QSQL_WORKER_MEMORY="${1}"
            shift  
            ;;
        esac
        ;;
        --driver_memory)
        shift ;
        case "${1}" in
            "")
            echo "ERROR: No --driver_memory selected, decide to default driver_memory";
            QSQL_DRIVER_MEMORY="${QSQL_DEFAULT_DRIVER_MEMORY}"
            shift  
            ;;
            *)
            QSQL_DRIVER_MEMORY="${1}"
            shift  
            ;;
        esac
        ;;
        --worker_num)
        shift;
        case "${1}" in
            "")
            echo "ERROR: No --worker_num selected, decide to default worker_num";
            QSQL_WORKER_NUM="${QSQL_DEFAULT_WORKER_NUM}"
            shift  
            ;;
            *)
            QSQL_WORKER_NUM="${1}"
            shift  
            ;;
        esac
        ;;
        -h|--help)
        shift; 
        HELP_ENABLE="true" 
        ;;
        --)
        shift
        break
        ;;
    esac
done

if [ ! -z "${HELP_ENABLE}" ] ; then
    echo "Options:
    -e SQL statements,Required.
    --runner Specify the compute engine, including spark, flink， jdbc or dynamic (Default: dynamic).
    --master Specify the execution mode on cluster, including yarn-client or yarn-cluster (Default: yarn-client).
    --worker_num Number of workers for launching (Default: 20).
    --worker_memory Memory per worker (e.g., 500M, 2G) (Default: 1G).
    --driver_memory Memory for driver (e.g., 500M, 2G) (Default: 3G).
    "
    exit 1
fi

function buildFatJar(){
  JAR_FILE="${QSQL_HOME}/lib/flink/qsql-core-0.6.jar"

 if [ ! -f "${JAR_FILE}" ]; then
    echo "INFO: Start build flink fat jar...";
    mkdir "${QSQL_HOME}/lib/flink/tmp"

    cp "${QSQL_HOME}"/lib/flink/*.jar "${QSQL_HOME}"/lib/flink/tmp/
    if [ -z "${HADOOP_HOME}" ]
     then
       echo "WARN: HADOOP_HOME is not set";
    else
       cp "`find ${HADOOP_HOME}/ -name "hadoop-common*.jar" |head -n 1`"  "${QSQL_HOME}"/lib/flink/tmp
    fi

    cd "${QSQL_HOME}"/lib/flink/tmp

    for jarFile in "$(ls *.jar | grep -v "qsql-core-0.6-fat.jar")"; do
        jar -xf "${jarFile}"
    done
    rm -rf META-INF

    jar -xf qsql-core-0.6-fat.jar
    if [ $? -ne 0 ]; then
       echo "ERROR: File qsql-core-0.6-fat.jar is not exist.";
       exit 1
    fi

    ls |grep -v jar$ | xargs jar cfM qsql-core-0.6.jar
    if [ $? -ne 0 ]; then
       echo "ERROR: Failed buid flink fat jar qsql-core-0.6.jar.";
       exit 1
    fi

    mv qsql-core-0.6.jar "${QSQL_HOME}"/lib/flink
    cd "${QSQL_HOME}"
    rm -rf "${QSQL_HOME}"/lib/flink/tmp
    echo "INFO: End build flink fat jar.";
 fi

}

if [ ! -z "${QSQL_RUNNER}" ] ; then
    CONF="${CONF}--runner=${QSQL_RUNNER} "
else
    CONF=${CONF}"--runner=${QSQL_DEFAULT_RUNNER} "
fi

if [ ! -z "${QSQL_MASTER}" ] ; then
    CONF=${CONF}"--master=${QSQL_MASTER} "
else
    CONF=${CONF}"--master=${QSQL_DEFAULT_MASTER} "
fi

if [ ! -z "${QSQL_WORKER_MEMORY}" ] ; then
    CONF=${CONF}"--worker_memory=${QSQL_WORKER_MEMORY} "
else
    CONF=${CONF}"--worker_memory=${QSQL_DEFAULT_WORKER_MEMORY} "
fi

if [ ! -z "${QSQL_DRIVER_MEMORY}" ] ; then
    CONF=${CONF}"--driver_memory=${QSQL_DRIVER_MEMORY} "
else
    CONF=${CONF}"--driver_memory=${QSQL_DEFAULT_DRIVER_MEMORY} "
fi

if [ ! -z "${QSQL_WORKER_NUM}" ] ; then
    CONF=${CONF}"--worker_num=${QSQL_WORKER_NUM} "
else
    CONF=${CONF}"--worker_num=${QSQL_DEFAULT_WORKER_NUM} "
fi

#. "${QSQL_HOME}/bin/quicksql.sh-env"

if [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    JAVA_RUNNER="${JAVA_HOME}/bin/java"
else
    if [ `command -v java` ]; then
        JAVA_RUNNER="java"
    else
        echo "ERROR: JAVA_HOME is not set" >&2
        exit 1
    fi
fi

if [[ "$JAVA_RUNNER" ]]; then
    version=$("$JAVA_RUNNER" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    IFS=.
    read major minor extra <<<"$version";
    if (( major == 1 && minor < 8 ));
    then
        echo "ERROR: Required java version >= 1.8"
        exit 1
    fi
fi


if [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    JAVA_RUNNER="${JAVA_HOME}/bin/java"
else
    if [ `command -v java` ]; then
        JAVA_RUNNER="java"
    else
        echo "ERROR: JAVA_HOME is not set" >&2
        exit 1
    fi
fi

if [[ "$JAVA_RUNNER" ]]; then
    version=$("$JAVA_RUNNER" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    IFS=.
    read major minor extra <<<"$version";
    if (( major == 1 && minor < 8 ));
    then
        echo "ERROR: Required java version >= 1.8"
        exit 1
    fi
fi

if [ "${QSQL_RUNNER}"X = "FLINKX" ] ; then
   if [[ -n "$FLINK_HOME" ]] && [[ -x "$FLINK_HOME/bin/flink" ]];  then
      FLINK_RUNNER="${FLINK_HOME}/bin/flink"
   else
      if [ `command -v flink` ]; then
         FLINK_RUNNER="flink run"
      else
        echo "ERROR: FLINK_HOME is not set" >&2
        exit 1
    fi
   fi
  buildFatJar
  CONF=${CONF}" --jar_name=${QSQL_HOME}/lib/flink/qsql-core-0.6.jar "

elif [ "${QSQL_RUNNER}"X = "SPARKX" ] ; then

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
   CONF=${CONF}" --jar_name=${QSQL_HOME}/lib/qsql-core-0.6.jar "
else
   CONF=${CONF}" --jar_name=${QSQL_HOME}/lib/qsql-core-0.6.jar "
fi

CONF=${CONF}" --class_name=com.qihoo.quicksql.sh.cli.QSqlSubmit "
CONF=${CONF}" --jar=${JARS} "

QSQL_LAUNCH_CLASSPATH="${QSQL_HOME}/lib/qsql-core-0.6.jar"
QSQL_LAUNCH_CLASSPATH="${QSQL_LAUNCH_CLASSPATH}:${QSQL_JARS}"


if [ ! -z "${SQL}" ] ; then
    SQL=$(echo "${SQL}" | base64 -w 0)
    CONF=${CONF}"--sql=${SQL}"
    eval "${JAVA_RUNNER}" -cp "${QSQL_LAUNCH_CLASSPATH}" com.qihoo.qsql.launcher.ExecutionDispatcher " $CONF"
    exit $?
fi
