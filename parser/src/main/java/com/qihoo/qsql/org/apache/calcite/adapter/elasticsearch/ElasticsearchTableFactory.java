package com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;
import com.qihoo.qsql.org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class ElasticsearchTableFactory implements TableFactory<ElasticsearchTable> {
    @Override
    public ElasticsearchTable create(SchemaPlus schema, String tableName,
                                     Map<String, Object> operand, RelDataType rowType) {
        final ElasticsearchSchema esSchema = schema.unwrap(ElasticsearchSchema.class);
        ElasticsearchTable table = new ElasticsearchTranslatableTable(esSchema.getClient(), new ObjectMapper(),
                esSchema.getIndex(), operand);
        esSchema.addTable(tableName, table);
        return table;
    }
}
