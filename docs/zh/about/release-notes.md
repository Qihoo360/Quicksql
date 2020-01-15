[English](../../about/release-notes.md)|[中文](./release-notes.md)

# **RELEASE-0.7**

**0.7.0版涉及的改动如下：**

**[核心模块]]**

- 引入了 Flink 计算引擎，使 Flink 也可以作为中间引擎进行联查

- 丰富接入方式，实现基于 Avatica 改造的 JDBC 接入方式

- 实现 DDL 查询 SHOW TABLES/SHOW DATABASES/DESCRIBE 语法

- 完善计算逻辑的下推能力，对于谓词关联比较等场景进行了深度优化

- 进行 TPC-DS 测试并修复部分语法问题

- 支持通过 Docker 构建项目

- 整理归档查询脚本，统一脚本命名

- 支持CI构建

**[适配器模块]**

- 支持 Apache Kylin 数据源查询

- 支持 Hive-JDBC 数据源查询

# **RELEASE-0.6**

**0.6版涉及的改动如下：**

**[核心模块]**

- 优化了部署流程，增加了元数据脚本化采集，接入时间可以缩短为原先的1/20。

- 支持函数的控制，数据源不支持的函数可以推向底层引擎执行。

- 支持通过 INSERT INTO 的方式将结果落地到HDFS。

- 支持表格化输出查询结果。

**[适配器模块]**

- 支持 Oracle 查询。

- 支持 Elasticsearch 执行 Join 和 Union 操作。

- 支持 Elasticsearch 在混算场景下的聚合操作 (此前只支持 JDBC 查询下的聚合)。

- 支持混算模式下 JDBC 数据源的分布式查询。

# **RELEASE-0.5**
- 无