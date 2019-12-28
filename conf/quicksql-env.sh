#!/bin/bash

# This file is sourced when running quicksql programs
# Copy it as quicksql-env.sh and edit it that to configure quicksql

# Options read when launching programs

# export SPARK_HOME=  # [Required] - SPARK_HOME, to set spark home for quicksql running. quicksql needs spark 2.0 or above.
# export JAVA_HOME=   # [Required] - JAVA_HOME, to set java home for quicksql running. quicksql needs java 1.8 or above.
# export FLINK_HOME=   # [Required] - FLINK_HOME, to set flink home for quicksql running. quicksql needs flink 1.9.0 or
# above.
# export QSQL_CLUSTER_URL=  # [Required] - QSQL_CLUSTER_URL, to set hadoop file system url.
# export QSQL_HDFS_TMP=   # [Required] - QSQL_HDFS_TMP, to set hadoop file system tmp url.

# Options read when using command line "quicksql.sh -e" with runner "Jdbc", "Spark" or "Dynamic" runner determined that
# this
# They all have default values. But we recommend you to set them here for more properly to your site.
# Those are default value for running a quicksql program if user does not set those properties.

# export QSQL_DEFAULT_WORKER_NUM=20      # [Optional] - QSQL_DEFAULT_WORKER_NUM, to set default worker_num for quicksql programs. if it is not set, default value is
# export QSQL_DEFAULT_WORKER_MEMORY=1G   # [Optional] - QSQL_DEFAULT_WORKER_MEMORY, to set default worker_memory for quicksql programs. if it is not set, default
# export QSQL_DEFAULT_DRIVER_MEMORY=3G   # [Optional] - QSQL_DEFAULT_DRIVER_MEMORY, to set default driver_memory for quicksql programs. if it is not set, default
# export QSQL_DEFAULT_MASTER=yarn-client # [Optional] - QSQL_DEFAULT_MASTER, to set default master for quicksql programs. if it is not set, default value is
# export QSQL_DEFAULT_RUNNER=DYNAMIC     # [Optional] - QSQL_DEFAULT_RUNNER, to set default master for quicksql programs. if it is not set, default value is dynamic.
