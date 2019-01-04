package com.qihoo.qsql.codegen.flink;

import com.qihoo.qsql.codegen.QueryGenerator;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.flink.FlinkPipeline} is chosen and source data of query is in
 * Elasticsearch at the same time.
 */
public class FlinkElasticsearchGenerator extends QueryGenerator {

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
