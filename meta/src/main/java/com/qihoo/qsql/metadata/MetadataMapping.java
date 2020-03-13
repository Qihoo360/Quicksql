package com.qihoo.qsql.metadata;

import com.qihoo.qsql.org.apache.calcite.tools.YmlUtils;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
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
            "esUser", "esPass", "esIndex", "esScrollNum"),
        Collections.singletonList(new SimpleImmutableEntry<>("esIndex", "dbName%/%tableName"))),
    /**
     * use '%' and literal 'value' to complete mapping.
     */
    MONGODB("com.qihoo.qsql.org.apache.calcite.adapter.mongodb.MongoSchemaFactory",
        "com.qihoo.qsql.org.apache.calcite.adapter.mongodb.MongoTableFactory",
        Arrays.asList(
            "dbName", "collectionName", "dbType", "host",
            "port", "userName", "password","authMechanism"),
        Collections.emptyList()
    ),

    JDBC("com.qihoo.qsql.org.apache.calcite.adapter.custom.JdbcSchemaFactory",
        "com.qihoo.qsql.org.apache.calcite.adapter.custom.JdbcTableFactory",
        Arrays.asList(
            "dbName", "tableName", "dbType", "jdbcDriver",
            "jdbcUrl", "jdbcUser", "jdbcPassword"),
        Collections.emptyList()
        ),

    Hive("com.qihoo.qsql.org.apache.calcite.adapter.hive.HiveSchemaFactory",
        "com.qihoo.qsql.org.apache.calcite.adapter.hive.HiveTableFactory",
        Arrays.asList(
            "dbName", "tableName", "cluster"),
        Collections.emptyList());

    public static final String HIVE = "hive";
    public static final String MONGO = "mongo";
    public static final String ELASTICSEARCH = "es";

    private static final String JOINT_FLAG = "%";
    String schemaClass;
    String tableClass;
    List<String> calciteProperties;
    List<AbstractMap.SimpleImmutableEntry<String, String>> componentProperties;

    MetadataMapping(String schemaClass, String tableClass,
        List<String> calciteProperties, List<AbstractMap.SimpleImmutableEntry<String, String>> componentProperties) {
        this.schemaClass = schemaClass;
        this.tableClass = tableClass;
        this.calciteProperties = calciteProperties;
        this.componentProperties = componentProperties;
    }

    static MetadataMapping convertToAdapter(String name) {
        Map<String, Map<String,String>> sourceMap = YmlUtils.getSourceMap();
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

    void completeComponentProperties(Map<String, String> properties) {
        componentProperties.forEach(entry -> properties.put(entry.getKey(),
            extractAssetMetadata(entry.getValue(), properties)));
        String dbValue = properties.getOrDefault(calciteProperties.get(0), "");
        String tableValue = properties.getOrDefault(calciteProperties.get(1), "");
        properties.put(calciteProperties.get(0), dbValue);
        properties.put(calciteProperties.get(1), tableValue);
    }

    private String extractAssetMetadata(String key, Map<String, String> parameters) {
        if (!key.contains(JOINT_FLAG)) {
            return parameters.getOrDefault(key, "");
        }

        String[] arr = key.split(JOINT_FLAG);
        return Arrays.stream(arr).map(param -> parameters.getOrDefault(param, param))
            .reduce((left, right) -> left + right).orElse("");
    }
}
