[English](./release-notes.md)|[中文](../zh/about/release-notes.md)

# **RELEASE-0.7**

**The changes involved in version 0.7 are as follows:**

**[Core Module]**

- Supported FLink engine

- Supported JDBC

- Implemented `SHOW TABLES`/`SHOW DATABASES`/`DESCRIBE` syntax

- Deep optimization for scenarios such as predicate association comparison

- Perform TPC-DS test and fix some syntax problems

- Support docker

- Organize archive query scripts and unified script naming

- Support CI

**[Adapter Module]**

- Support Apache Kylin data source

- Support Hive JDBC data source

# **RELEASE-0.6**

**The changes involved in version 0.6 are as follows:**

**[Core Module]**

- Optimized the deployment process, increased the scripted collection of metadata, and shortened the access time to the original 1/20.

- Support control of functions. Functions not supported by the data source can be pushed to the underlying engine for execution.

- Support inserting results to HDFS via INSERT INTO.

- Support tabular output of query results.

**[Adapter Module]**

- Support Oracle.

- Support Elasticsearch to perform Join and Union operations.

- Support the aggregation operation of Elasticsearch in mixed calculation scenarios (previously only supported the aggregation under JDBC query).

- Support distributed query of JDBC data source in mixed calculation mode.

# **RELEASE-0.5**
-None
