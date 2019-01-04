package com.qihoo.qsql.plan.proc;

import com.qihoo.qsql.plan.ProcedureVisitor;
import java.util.Properties;

/**
 * ExtractProcedure, use for calculating and extracting data in each of data source.
 */
public abstract class ExtractProcedure extends QueryProcedure {

    private Properties connectionProperties;
    private String tableName;

    /**
     * Extract Procedure, correspond to data engine.
     *
     * @param next next Procedure
     * @param properties properties of Procedure
     * @param tableName tableName of Procedure
     */
    public ExtractProcedure(QueryProcedure next,
        Properties properties,
        String tableName) {
        super(next);
        this.connectionProperties = properties;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public abstract String toRecognizedQuery();

    public Properties getConnProperties() {
        return connectionProperties;
    }

    @Override
    public int getValue() {
        return 0x10;
    }

    public abstract String getCategory();

    @Override
    public void accept(ProcedureVisitor visitor) {
        visitor.visit(this);
    }
}
