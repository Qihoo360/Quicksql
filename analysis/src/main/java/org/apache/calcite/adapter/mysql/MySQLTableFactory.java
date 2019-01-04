package org.apache.calcite.adapter.mysql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class MySQLTableFactory implements TableFactory {

    @Override
    public Table create(SchemaPlus schema, String name, Map operand, RelDataType rowType) {
        String tableName = operand.get("tableName").toString();
        String dbName = operand.get("dbName").toString();
        String jdbcUrl = operand.get("jdbcUrl").toString();
        String jdbcUser = operand.get("jdbcUser").toString();
        String jdbcPassword = operand.get("jdbcPassword").toString();
        String jdbcDriver = operand.get("jdbcDriver").toString();
        String modelUri = operand.get("modelUri").toString();
        
        return new MySQLTable(tableName, dbName,
            jdbcDriver, jdbcUrl,
            jdbcUser, jdbcPassword,
            modelUri);
    }
}