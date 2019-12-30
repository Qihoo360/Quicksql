package com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;
import com.qihoo.qsql.org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class ElasticsearchTableFactory implements TableFactory<ElasticsearchTable> {
    @Override
    public ElasticsearchTable create(SchemaPlus schema, String name,
                                     Map<String, Object> operand, RelDataType rowType) {
        final ElasticsearchSchema esSchema = schema.unwrap(ElasticsearchSchema.class);
        String type = operand.getOrDefault("tableName", "").toString();
        ElasticsearchTable table = new ElasticsearchTranslatableTable(esSchema.getClient(), new ObjectMapper(),
                esSchema.getIndex(), type, operand);
        esSchema.addTable(type, table);
        return table;
    }
}
