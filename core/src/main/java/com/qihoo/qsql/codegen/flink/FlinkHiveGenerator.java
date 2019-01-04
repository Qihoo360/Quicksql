package com.qihoo.qsql.codegen.flink;

import com.qihoo.qsql.codegen.QueryGenerator;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.flink.FlinkPipeline} is chosen and source data of query is in
 * Hive at the same time.
 */
public class FlinkHiveGenerator extends QueryGenerator {

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
