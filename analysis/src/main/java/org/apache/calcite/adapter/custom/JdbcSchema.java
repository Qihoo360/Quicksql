package org.apache.calcite.adapter.custom;

import org.apache.calcite.schema.impl.AbstractSchema;

//TODO reduce all of default schemas like this which has no field and param
public class JdbcSchema extends AbstractSchema {
    public JdbcSchema() {}
}
