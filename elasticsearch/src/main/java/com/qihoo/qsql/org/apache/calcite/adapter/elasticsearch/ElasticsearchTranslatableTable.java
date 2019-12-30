package com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qihoo.qsql.org.apache.calcite.model.ModelHandler;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.elasticsearch.client.RestClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ElasticsearchTranslatableTable extends ElasticsearchTable {
    private Map<String, Object> operand;

    ElasticsearchTranslatableTable(RestClient client, ObjectMapper mapper,
                                   String indexName, String typeName, Map<String, Object> operand) {
        super(client, mapper, indexName, typeName);
        this.operand = operand;
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        operand.forEach((key, value) -> properties.put(key, value.toString()));
        return properties;
    }

    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        String modelUri = operand.getOrDefault(
                ModelHandler.ExtraOperand.MODEL_URI.camelName, "").toString();
        return super.getRowType(modelUri, operand.getOrDefault("dbName", "").toString(),
                operand.getOrDefault("tableName", "").toString(), relDataTypeFactory);
    }

    @Override
    public String getBaseName() {
        return operand.getOrDefault("dbName", "").toString();
    }
}
