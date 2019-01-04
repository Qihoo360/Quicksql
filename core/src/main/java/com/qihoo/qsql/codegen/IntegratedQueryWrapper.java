package com.qihoo.qsql.codegen;

import com.qihoo.qsql.plan.proc.QueryProcedure;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provide several method, which can generate execution code in intermediate engine layer.
 * <p>
 * For example, the results calculated in different data source, will be saved in different temp tables in engine(e .g.,
 * Spark), then the engine will calculate final result based on those temp table and show the result in console finally.
 * {@link IntegratedQueryWrapper} provides methods for engine to do those different actions.
 * </p>
 */

public abstract class IntegratedQueryWrapper extends ClassBodyWrapper {

    protected static final String VARIABLE_PREFIX = "$";
    protected AtomicInteger varId = new AtomicInteger(0);

    public abstract IntegratedQueryWrapper run(QueryProcedure plan);

    public abstract void interpretProcedure(QueryProcedure plan);

    public abstract void importSpecificDependency();

    public abstract IntegratedQueryWrapper show();

    public abstract IntegratedQueryWrapper writeAsTextFile(String path, String deliminator);

    public abstract IntegratedQueryWrapper writeAsJsonFile(String path);

    public abstract void createTempTable(String tableName);

    protected String latestDeclaredVariable() {
        return VARIABLE_PREFIX + varId.get();
    }
}
