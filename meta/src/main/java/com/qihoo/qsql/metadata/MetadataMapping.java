package com.qihoo.qsql.metadata;

import com.qihoo.qsql.org.apache.calcite.tools.JdbcSourceInfo;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Provide different params adapt for type of data source.
 */
public enum MetadataMapping {
    Elasticsearch("com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch.ElasticsearchCustomSchemaFactory",
        "com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch.ElasticsearchTableFactory",
        Arrays.asList(
            "dbName", "tableName", "esNodes", "esPort",
            "esUser", "esPass", "esName", "esScrollNum")
    ),
    /**
     * use '%' and literal 'value' to complete mapping.
     */
    MONGODB("com.qihoo.qsql.org.apache.calcite.adapter.mongodb.MongoSchemaFactory",
        "com.qihoo.qsql.org.apache.calcite.adapter.mongodb.MongoTableFactory",
        Arrays.asList(
            "dbName", "collectionName", "dbType", "host",
            "port", "userName", "password","authMechanism")
    ),

    JDBC("com.qihoo.qsql.org.apache.calcite.adapter.custom.JdbcSchemaFactory",
        "com.qihoo.qsql.org.apache.calcite.adapter.custom.JdbcTableFactory",
        Arrays.asList(
            "dbName", "tableName", "dbType", "jdbcDriver",
            "jdbcUrl", "jdbcUser", "jdbcPassword")
        ),

    Hive("com.qihoo.qsql.org.apache.calcite.adapter.hive.HiveSchemaFactory",
        "com.qihoo.qsql.org.apache.calcite.adapter.hive.HiveTableFactory",
        Arrays.asList(
            "dbName", "tableName", "cluster")
    );

    public static final String HIVE = "hive";
    public static final String MONGO = "mongo";
    public static final String ELASTICSEARCH = "es";

    private static final String JOINT_FLAG = "%";
    String schemaClass;
    String tableClass;
    List<String> calciteProperties;

    MetadataMapping(String schemaClass, String tableClass,
        List<String> calciteProperties) {
        this.schemaClass = schemaClass;
        this.tableClass = tableClass;
        this.calciteProperties = calciteProperties;
    }

    static MetadataMapping convertToAdapter(String name) {
        Map<String, Map<String,String>> sourceMap = JdbcSourceInfo.getSourceMap();
        if (sourceMap.containsKey(name.toLowerCase())) {
            return MetadataMapping.JDBC;
        }
        switch (name.toLowerCase()) {
            case ELASTICSEARCH:
                return MetadataMapping.Elasticsearch;
            case HIVE:
                return MetadataMapping.Hive;
            case MONGO:
                return MetadataMapping.MONGODB;
            default:
                throw new RuntimeException("Not support given adapter name!!");
        }
    }
}
