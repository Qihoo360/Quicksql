#!/bin/bash

# This file is sourced when running qsql programs
# Copy it as base-env.sh and edit it that to configure qsql

# Options read when launching programs

# export SPARK_HOME=  # [Required] - SPARK_HOME, to set spark home for qsql running. qsql needs spark 2.0 or above.
# export JAVA_HOME=   # [Required] - JAVA_HOME, to set java home for qsql running. qsql needs java 1.8 or above.
# export QSQL_CLUSTER_URL=  # [Required] - QSQL_CLUSTER_URL, to set hadoop file system url.
# export QSQL_HDFS_TMP=   # [Required] - QSQL_HDFS_TMP, to set hadoop file system tmp url.

# Options read when using command line "qsql -e" with runner "Jdbc", "Spark" or "Dynamic" runner determined that this
# They all have default values. But we recommend you to set them here for more properly to your site.
# Those are default value for running a qsql program if user does not set those properties.

# export QSQL_DEFAULT_WORKER_NUM=20      # [Optional] - QSQL_DEFAULT_WORKER_NUM, to set default worker_num for qsql programs. if it is not set, default value is
# export QSQL_DEFAULT_WORKER_MEMORY=1G   # [Optional] - QSQL_DEFAULT_WORKER_MEMORY, to set default worker_memory for qsql programs. if it is not set, default
# export QSQL_DEFAULT_DRIVER_MEMORY=3G   # [Optional] - QSQL_DEFAULT_DRIVER_MEMORY, to set default driver_memory for qsql programs. if it is not set, default
# export QSQL_DEFAULT_MASTER=yarn-client # [Optional] - QSQL_DEFAULT_MASTER, to set default master for qsql programs. if it is not set, default value is
# export QSQL_DEFAULT_RUNNER=DYNAMIC     # [Optional] - QSQL_DEFAULT_RUNNER, to set default master for qsql programs. if it is not set, default value is dynamic.
