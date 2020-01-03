#!/bin/bash


export QSQL_HOME="$(cd "`dirname "$0"`"/..; pwd)"
 . "${QSQL_HOME}/bin/commons.sh"

#parse args
ARGS=`getopt -o "h" -l "dbType:,action:" -n "qsql" -- "$@"`

eval set -- "${ARGS}"

while true
do
    case "${1}" in
        --dbType)
        shift 
        case "${1}" in
            "")
            echo "no dbType selected, please select one dbType";
            exit 1
            ;;
            "mysql")
            QSQL_DB_TYPE="mysql"
            shift ;
            ;;
            *)
            echo "dbType Error! please select property dbType!"
            exit 1
            ;;
        esac
        ;;
        --action)
        shift ;
        case "${1}" in
            "")
            echo "no action selected, please select one action";
            exit 1
            ;;
            "init")
            QSQL_ACTION="init"
            shift;
            ;;
            "delete")
            QSQL_ACTION="delete"
            shift;
            ;;
            *)
            echo "runner error! please select property runner!"
            exit 1
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
    --dbType Set external metadata-init.sh storage type. e.g., mysql
    --action Set action for external metadata-init.sh storage. including init or delete.
    "
    exit 1
fi

if [ ! -z "${QSQL_DB_TYPE}" ] ; then
    CONF=${CONF}"--dbType=${QSQL_DB_TYPE} "
else
    echo "no dbType selected, please select one dbType"
    exit 1
fi

if [ ! -z "${QSQL_ACTION}" ] ; then
    CONF=${CONF}"--action=${QSQL_ACTION} "
else
    echo "no action selected, please select one action"
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

QSQL_METADATA_CLASSPATH="${QSQL_HOME}/lib/qsql-core-"${PROJECT_VERSION}".jar"
QSQL_METADATA_CLASSPATH="${QSQL_HOME}/lib/mysql-connector-java-5.1.20.jar:${QSQL_METADATA_CLASSPATH}"
QSQL_METADATA_CLASSPATH="${QSQL_HOME}/lib/ibatis-core-3.0.jar:${QSQL_METADATA_CLASSPATH}"
QSQL_METADATA_CLASSPATH="${QSQL_HOME}/lib/commons-lang3-3.8.jar:${QSQL_METADATA_CLASSPATH}"
QSQL_METADATA_CLASSPATH="${QSQL_HOME}/lib/qsql-meta-"${PROJECT_VERSION}".jar:${QSQL_METADATA_CLASSPATH}"



echo "start action..."

"${JAVA_RUNNER}" -cp "${QSQL_METADATA_CLASSPATH}" com.qihoo.qsql.metadata.extern.MetadataMain ${CONF}

echo "end action ..."
