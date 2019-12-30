#!/bin/bash
unset QSQL_JARS

if [ -z "${QSQL_HOME}" ]
then
    export QSQL_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

for jar in `find ${QSQL_HOME}/lib -maxdepth 1 -name "*.jar"`
do
    if [ ! -n "${QSQL_JARS}" ]
    then
        export QSQL_JARS="${jar}"
    else
        export QSQL_JARS="${QSQL_JARS}:${jar}"
    fi
done

for jar in `find ${QSQL_HOME}/lib/spark -name "*.jar"`
do
    if [  !  -n  "${JARS}"  ]
    then
        export JARS="${jar}"
    else
        export JARS="${JARS},${jar}"
    fi
done

user_conf_dir="${QSQL_HOME}/conf"
if [ -f "${user_conf_dir}/quicksql-env.sh" ]
then
    set -a
    . "${user_conf_dir}/quicksql-env.sh"
    set +a
fi


