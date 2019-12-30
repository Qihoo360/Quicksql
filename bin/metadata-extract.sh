#!/bin/bash

export QSQL_HOME="$(cd "`dirname "$0"`"/..; pwd)"
. "${QSQL_HOME}/bin/commons.sh"

ARGS=`getopt -o "p:d:r:h" -n "metadata-extract.sh" -- "$@"`

eval set -- "${ARGS}"
# eval is evil

while true
do
    case "${1}" in
        -p)
        shift;
        PROPERTY=${1}
        shift;
        ;;
        -d)
        shift;
        DATA_SOURCE=${1}
        shift;
        ;;
        -r)
        shift;
        MATCHER=${1}
        shift;
        ;;
        -h)
        shift;
        HELP_ENABLE="true"
        ;;
        --)
        shift
        break
        ;;
    esac
done

if [ -z "${PROPERTY}" ]; then
    echo "ERROR: Connection information is necessary. Please read doc"
    exit 1
fi

if [ -z "${DATA_SOURCE}" ]; then
    echo "ERROR: Data source type is necessary. e.g.: es, mysql"
    exit 1
fi

if [ -z "${MATCHER}" ]; then
    MATCHER="%%"
fi

if [ ! -z "${HELP_ENABLE}" ]; then
    echo "Options:
    -p Server connection property in JSON format. (e.g.: {\"jdbcDriver\": \"driver\", \"jdbcUrl\": \"localhost\"} )
    -d Data source type. (e.g.: ES, MySQL)
    -r Table name fuzzy matching rule, support operators (%, _, ?)
    "
    exit 1
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
    OLD_IFS=$IFS
    IFS=.
    read major minor extra <<<"$version";
    IFS=$OLD_IFS
    if (( major == 1 && minor < 8 ));
    then
        echo "ERROR: Required java version >= 1.8"
        exit 1
    fi
fi

#for jar in `find "${QSQL_HOME}/lib" -maxdepth 1 -name "*.jar"`
#do
#    if [ ! -n "${QSQL_JARS}" ]
#    then
#        export QSQL_JARS="${jar}"
#    else
#        export QSQL_JARS="${QSQL_JARS}:${jar}"
#    fi
#done

"${JAVA_RUNNER}" -cp "${QSQL_JARS}" com.qihoo.qsql.metadata.collect.MetadataCollector \
"${PROPERTY}" "${DATA_SOURCE}" "${MATCHER}"

exit 0
