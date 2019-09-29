package com.qihoo.qsql.metadata;

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
    Elasticsearch("org.apache.calcite.adapter.elasticsearch.ElasticsearchCustomSchemaFactory",
        "org.apache.calcite.adapter.elasticsearch.ElasticsearchTableFactory",
        Arrays.asList(
            "dbName", "tableName", "esNodes", "esPort",
            "esUser", "esPass", "esIndex", "esScrollNum"),
        Collections.singletonList(new SimpleImmutableEntry<>("esIndex", "dbName%/%tableName"))),
    /**
     * use '%' and literal 'value' to complete mapping.
     */
    JDBC("org.apache.calcite.adapter.custom.JdbcSchemaFactory",
        "org.apache.calcite.adapter.custom.JdbcTableFactory",
        Arrays.asList(
            "dbName", "tableName", "dbType", "jdbcDriver",
            "jdbcUrl", "jdbcUser", "jdbcPassword"),
        Collections.emptyList()
        ),

    Hive("org.apache.calcite.adapter.hive.HiveSchemaFactory",
        "org.apache.calcite.adapter.hive.HiveTableFactory",
        Arrays.asList(
            "dbName", "tableName", "cluster"),
        Collections.emptyList());

    public static final String ELASTICSEARCH = "es";
    public static final String MYSQL = "mysql";
    public static final String KYLIN = "kylin";
    public static final String ORACLE = "oracle";
    public static final String HIVE = "hive";

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
        switch (name.toLowerCase()) {
            case ELASTICSEARCH:
                return MetadataMapping.Elasticsearch;
            case MYSQL:
            case KYLIN:
            case ORACLE:
                return MetadataMapping.JDBC;
            case HIVE:
                return MetadataMapping.Hive;
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
        if (! key.contains(JOINT_FLAG)) {
            return parameters.getOrDefault(key, "");
        }

        String[] arr = key.split(JOINT_FLAG);
        return Arrays.stream(arr).map(param -> parameters.getOrDefault(param, param))
            .reduce((left, right) -> left + right).orElse("");
    }
}
