package com.qihoo.qsql.exec.spark;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.codegen.spark.SparkBodyWrapper;
import com.qihoo.qsql.exec.AbstractPipeline;
import com.qihoo.qsql.exec.Compilable;
import com.qihoo.qsql.exec.result.JobPipelineResult;
import com.qihoo.qsql.exec.result.PipelineResult;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pipeline special for Spark, that can concatenate all the steps in execution, including wrapping a Java Class code
 * executable in Spark, compiling to Class in memory, executing it and process the query result.
 */
public class SparkPipeline extends AbstractPipeline implements Compilable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkPipeline.class);

    /**
     * Pipeline of Spark.
     *
     * @param plan QueryProcedure
     * @param builder SparkSqlRunner builder
     */
    public SparkPipeline(QueryProcedure plan, SqlRunner.Builder builder) {
        super(plan, builder);
        wrapper = new SparkBodyWrapper();
        wrapper.interpretProcedure(plan);
        wrapper.importSpecificDependency();
    }

    @Override
    public void run() {
        SparkBodyWrapper newWrapper = new SparkBodyWrapper();
        try {
            compileRequirement(newWrapper.run(procedure), session(), SparkSession.class).execute();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private SparkSession session() {
        //TODO
        if (System.getenv("SPARK_HOME") != null) {
            SparkSession sc;
            if (builder.getEnableHive()) {
                sc = SparkSession.builder()
                    .appName(builder.getAppName())
                    .master(builder.getMaster())
                    .enableHiveSupport()
                    .getOrCreate();
            } else {
                sc = SparkSession.builder()
                    .appName(builder.getAppName())
                    .master(builder.getMaster())
                    .getOrCreate();
            }
            LOGGER
                .debug("Initialize SparkContext successfully, App name: {}", builder.getAppName());
            return sc;

        } else {
            LOGGER.error(
                "Initialize SparkContext failed, the reason for which is not find spark env");
            throw new RuntimeException("No available Spark to execute. Please deploy Spark and put SPARK_HOME in env");
        }

    }

    @Override
    public PipelineResult show() {
        return new JobPipelineResult.ShowPipelineResult(
            compileRequirement(wrapper.show(), session(), SparkSession.class));
    }

    @Override
    public PipelineResult asTextFile(String clusterPath, String deliminator) {
        return new JobPipelineResult.TextPipelineResult(clusterPath, deliminator,
            compileRequirement(wrapper.writeAsTextFile(clusterPath, deliminator), session(), SparkSession.class));
    }

    @Override
    public PipelineResult asJsonFile(String clusterPath) {
        return new JobPipelineResult.JsonPipelineResult(clusterPath,
            compileRequirement(wrapper.writeAsJsonFile(clusterPath), session(), SparkSession.class));
    }

    @Override
    public AbstractPipeline asTempTable(String tempTableName) {
        wrapper.createTempTable(tempTableName);
        return this;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public String source() {
        return wrapper.toString();
    }
}
