#!/bin/bash

export QSQL_HOME="$(cd "`dirname "$0"`"/..; pwd)"

if [ "$#"A = "0A" ] ; then
  HELP_ENABLE="true"
fi

#parse args
ARGS=`getopt -o "e:h"  -l "help:,runner:,class:" -n "quicksql-example.sh" -- "$@"`

. "${QSQL_HOME}/bin/commons.sh"

eval set -- "${ARGS}"

while true
do
    case "${1}" in
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
         --class)
        shift ;
        case "${1}" in
            "")
            echo "ERROR: No --class selected, please check";
            exit 1
            shift
            ;;
            *)
            QSQL_CLASS_NAME="${1}"
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
    --class Specify the application's main class.
    --runner Specify the compute engine, including spark, flink， jdbc or dynamic (Default: dynamic).
example:
    ./bin/quicksql-example.sh --class com.qihoo.qsql.CsvJoinWithEsExample --runner spark
    ./bin/quicksql-example.sh --class com.qihoo.qsql.SimpleQueryByFlinkExample --runner flink
    ./bin/quicksql-example.sh --class com.qihoo.qsql.CsvScanExample --runner spark "
    exit 1
fi


if [ -n "${JAVA_HOME}" ]; then
    JAVA_RUNNER="${JAVA_HOME}/bin/java"
else
    if [ `command -v java` ]; then
        JAVA_RUNNER="java"
    else
        echo "JAVA_HOME is not set" >&2
        exit 1
    fi
fi


if [ "${QSQL_RUNNER}"X = "FLINKX" ] ; then

   if [ -n "${FLINK_HOME}" ]; then
        FLINK_JARS_LIBS="${FLINK_HOME}/lib"
    else
        echo "FLINK_HOME is not set" >&2
        exit 1
    fi

    for jar in `find ${FLINK_JARS_LIBS} -name "*.jar"`
    do
        if [  !  -n  "${EXP_JARS}"  ]
        then
            export EXP_JARS="${jar}"
        else
            export EXP_JARS="${EXP_JARS}:${jar}"
        fi
    done

elif [ "${QSQL_RUNNER}"X = "SPARKX" ] ; then

    if [[ -n "$SPARK_HOME" ]] && [[ -x "$SPARK_HOME/bin/spark-submit" ]];  then
        SPARK_JARS_LIBS="${SPARK_HOME}/jars"
        SPARK_RUNNER="${SPARK_HOME}/bin/spark-submit"
    else
        echo "SPARK_HOME is not set" >&2
        exit 1
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

    for jar in `find ${SPARK_JARS_LIBS} -name "*.jar"`
    do
        if [  !  -n  "${EXP_JARS}"  ]
        then
            export EXP_JARS="${jar}"
        else
            export EXP_JARS="${EXP_JARS}:${jar}"
        fi
    done
fi


QSQL_LAUNCH_CLASSPATH="${QSQL_JARS}:${EXP_JARS}"

"${JAVA_RUNNER}" -cp "${QSQL_LAUNCH_CLASSPATH}" ${QSQL_CLASS_NAME} ${QSQL_RUNNER}
