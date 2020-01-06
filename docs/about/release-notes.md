[English](./release-notes.md)|[中文](../zh/about/release-notes.md)

# **RELEASE-0.6**

**0.6版涉及的改动如下：**

**[核心模块]**

- 优化了部署流程，增加了元数据脚本化采集，接入时间可以缩短为原先的1/20。

- 支持函数的控制，数据源不支持的函数可以推向底层引擎执行。

- 支持通过INSERT INTO的方式将结果落地到HDFS。

- 支持表格化输出查询结果。

**[适配器模块]**

- 支持Oracle查询。

- 支持Elasticsearch执行Join和Union操作。

- 支持Elasticsearch在混算场景下的聚合操作 (此前只支持JDBC查询下的聚合)。

- 支持混算模式下JDBC数据源的分布式查询。

# **RELEASE-0.5**
- 无