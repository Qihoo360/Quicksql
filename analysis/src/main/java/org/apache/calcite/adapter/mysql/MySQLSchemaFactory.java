package org.apache.calcite.adapter.mysql;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class MySQLSchemaFactory implements SchemaFactory {

    public static final MySQLSchemaFactory INSTANCE = new MySQLSchemaFactory();

    private MySQLSchemaFactory() {}

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return new MySQLSchema();
    }
}
