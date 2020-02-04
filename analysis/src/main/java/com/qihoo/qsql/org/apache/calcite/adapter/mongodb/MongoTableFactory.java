package com.qihoo.qsql.org.apache.calcite.adapter.mongodb;


import java.util.Map;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;
import com.qihoo.qsql.org.apache.calcite.schema.Table;
import com.qihoo.qsql.org.apache.calcite.schema.TableFactory;

/**
 * Implementation of TableFactory for Hive
 * A table corresponds to what Hive calls a "data source"
 */
 //add by quicksql team
public class MongoTableFactory implements TableFactory {

    @Override
    public Table create(SchemaPlus schema, String name, Map operand, RelDataType rowType) {
        return new MongoTable(operand.get("collectionName").toString(),operand);
    }
}