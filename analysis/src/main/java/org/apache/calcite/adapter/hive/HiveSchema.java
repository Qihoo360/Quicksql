package org.apache.calcite.adapter.hive;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema is a CSV file in
 * that directory.
 */
public class HiveSchema extends AbstractSchema {

    private Map<String, Table> tableMap;

    public HiveSchema() {
    }
}
