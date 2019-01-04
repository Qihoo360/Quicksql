package com.qihoo.qsql.exec.spark;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.utils.SqlUtil;
import org.junit.Test;

public class SparkPipelineTest {

    @Test
    public void testSparkPipeline() {
        String sql = "select * from action_required.homework_content";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.SPARK;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql)))
            .setAppName("test hive")
            .setAcceptedResultsNum(10)
            .ok();
        runner.sql(sql).show();
    }

    @Test
    public void testSparkAsTextFilePipeline() {
        String sql = "select * from action_required.homework_content";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.SPARK;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql)))
            .setAppName("test hive")
            .setAcceptedResultsNum(10)
            .ok();
        runner.sql(sql).asTextFile("", "");
    }

    @Test
    public void testSparkAsTempTablePipeline() {
        String sql = "select * from action_required.homework_content";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.SPARK;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql)))
            .setAppName("test hive")
            .setAcceptedResultsNum(10)
            .ok();
        runner.sql(sql).asTempTable("temp").show();
    }

    @Test
    public void testSparkAsJsonFilePipeline() {
        String sql = "select * from action_required.homework_content";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.SPARK;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql)))
            .setAppName("test hive")
            .setAcceptedResultsNum(10)
            .ok();
        runner.sql(sql).asJsonFile("");
    }
}
