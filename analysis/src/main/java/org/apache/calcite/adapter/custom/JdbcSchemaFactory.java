package org.apache.calcite.adapter.custom;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class JdbcSchemaFactory implements SchemaFactory {

    public static final JdbcSchemaFactory INSTANCE = new JdbcSchemaFactory();

    private JdbcSchemaFactory() {}

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return new JdbcSchema();
    }
}
