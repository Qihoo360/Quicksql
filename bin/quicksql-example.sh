#!/bin/bash

export QSQL_HOME="$(cd "`dirname "$0"`"/..; pwd)"

#parse args
ARGS=`getopt -o "e:h"  -l "help:" -n "quicksql-example.sh" -- "$@"`

eval set -- "${ARGS}"

while true
do
    case "${1}" in
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

if [ "$#"A = "0A" ] ; then
  HELP_ENABLE="true"
fi

if [ ! -z "${HELP_ENABLE}" ] ; then
    echo "example:
    ./bin/quicksql-example.sh com.qihoo.qsql.CsvJoinWithEsExample
    ./bin/quicksql-example.sh com.qihoo.qsql.CsvScanExample  "
    exit 1
fi

. "${QSQL_HOME}/bin/commons.sh"
#. "${QSQL_HOME}/bin/qsql-env"

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

if [ -n "${SPARK_HOME}" ]; then
    SPARK_JARS_LIBS="${SPARK_HOME}/jars"
else
    echo "SPARK_HOME is not set" >&2
    exit 1
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

QSQL_LAUNCH_CLASSPATH="${QSQL_JARS}:${EXP_JARS}"

"${JAVA_RUNNER}" -cp "${QSQL_LAUNCH_CLASSPATH}" $1
