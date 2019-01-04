package com.qihoo.qsql.codegen.flink;

import com.qihoo.qsql.codegen.QueryGenerator;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.flink.FlinkPipeline} is chosen and when no table name is parsed
 * in query. For example, "select 1".
 */
public class FlinkVirtualGenerator extends QueryGenerator {

    @Override
    protected void importDependency() {

    }

    @Override
    protected void prepareQuery() {

    }

    @Override
    protected void executeQuery() {

    }

    @Override
    public void saveToTempTable() {

    }

}