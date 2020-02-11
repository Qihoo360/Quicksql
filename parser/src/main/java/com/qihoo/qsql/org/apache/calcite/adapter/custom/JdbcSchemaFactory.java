package com.qihoo.qsql.org.apache.calcite.adapter.custom;

import com.qihoo.qsql.org.apache.calcite.schema.Schema;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaFactory;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class JdbcSchemaFactory implements SchemaFactory {

    public static final JdbcSchemaFactory INSTANCE = new JdbcSchemaFactory();

    private JdbcSchemaFactory() {}

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return new JdbcSchema();
    }
}
