package org.apache.calcite.adapter.hive;


import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

/**
 * Implementation of TableFactory for Hive
 * A table corresponds to what Hive calls a "data source"
 */
public class HiveTableFactory implements TableFactory {

    @Override
    public Table create(SchemaPlus schema, String name, Map operand, RelDataType rowType) {
        String dbName = operand.get("dbName").toString();
        String cluster = operand.get("cluster").toString();
        String tableName = operand.get("tableName").toString();
        String modelUri = operand.get("modelUri").toString();
        return new HiveTable(dbName, cluster, tableName, modelUri);
    }
}
