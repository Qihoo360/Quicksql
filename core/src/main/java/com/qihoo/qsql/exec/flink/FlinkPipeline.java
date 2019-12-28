package com.qihoo.qsql.exec.flink;

import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.exec.AbstractPipeline;
import com.qihoo.qsql.exec.Compilable;
import com.qihoo.qsql.exec.result.PipelineResult;
import com.qihoo.qsql.codegen.flink.FlinkBodyWrapper;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * A pipeline special for Flink, that can concatenate all the steps in execution, including wrapping a Java Class code
 * executable in Flink, compiling to Class in memory, executing it and process the query result.
 */
public class FlinkPipeline extends AbstractPipeline implements Compilable {

    private ExecutionEnvironment executionEnvironment;
    private SqlRunner.Builder builder;

    /**
     * FlinkPipeline special for Flink Runner.
     *
     * @param plan QueryProcedure
     * @param builder Flink SqlRunner builder
     */
    public FlinkPipeline(QueryProcedure plan, SqlRunner.Builder builder) {
        super(plan, builder);
        wrapper = new FlinkBodyWrapper();
        wrapper.interpretProcedure(procedure);
        wrapper.importSpecificDependency();
    }

    @Override
    public void run() {

    }

    @Override
    public Object collect() {
        return null;
    }

    protected void startup() {
        executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
    }

    @Override
    public void show() {
        startup();
        wrapper.show();
        try {
            compileRequirement(wrapper, executionEnvironment, ExecutionEnvironment.class).execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public PipelineResult asTextFile(String clusterPath, String deliminator) {
        return null;
    }

    @Override
    public PipelineResult asJsonFile(String clusterPath) {
        return null;
    }

    @Override
    public AbstractPipeline asTempTable(String tempTableName) {
        return null;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public String source() {
        return wrapper.show().toString();
    }
}
