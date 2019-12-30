package com.qihoo.qsql.org.apache.calcite.adapter.hive;


import com.qihoo.qsql.org.apache.calcite.schema.Schema;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaFactory;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Factory that creates a HiveSchema
 * Allows a custom schema to be included in a model.json file
 */
public class HiveSchemaFactory implements SchemaFactory {

    /**
     * Name of the column that is implicitly created in a Hive stream table
     * to hold the data arrival time
     */
    static final String ROWTIME_COLUMN_NAME = "ROWTIME";

    /**
     * Public singleton, per factory contract.
     */
    public static final HiveSchemaFactory INSTANCE = new HiveSchemaFactory();

    private HiveSchemaFactory() {
    }


    /**
     * get HiveSchema
     */
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return new HiveSchema();
    }
}
