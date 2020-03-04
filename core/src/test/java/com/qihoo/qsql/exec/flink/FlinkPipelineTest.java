package com.qihoo.qsql.exec.flink;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.utils.SqlUtil;
import org.junit.Test;

public class FlinkPipelineTest {

    @Test
    public void testFlinkPipeline() {
        String sql = "select 1";
        RunnerType runnerType = RunnerType.FLINK;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql).tableNames))
            .setAppName("test hive")
            .setAcceptedResultsNum(10)
            .ok();
        runner.sql(sql).show();
    }

    @Test
    public void testFlinkAsTextFilePipeline() {
        String sql = "select * from action_required.homework_content";
        RunnerType runnerType = RunnerType.FLINK;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql).tableNames))
            .setAppName("test hive")
            .setAcceptedResultsNum(10)
            .ok();
        runner.sql(sql).asTextFile("", "");
    }

    @Test
    public void testFlinkAsJsonFilePipeline() {
        String sql = "select * from action_required.homework_content";
        RunnerType runnerType = RunnerType.FLINK;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql).tableNames))
            .setAppName("test hive")
            .setAcceptedResultsNum(10)
            .ok();
        runner.sql(sql).asJsonFile("");
    }
}
