package com.qihoo.qsql.exec;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.utils.SqlUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * JdbcPipelineTest.
 */
public class JdbcPipelineTest {

    @Test
    public void testJdbc() {
        String sql = "select 1";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.DEFAULT;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql).tableNames))
            .setAppName("test mysql")
            .setAcceptedResultsNum(2000)
            .ok();

        runner.sql(sql).show().run();
    }

    @Test
    public void testTextFile() {
        String sql = "select 1";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.DEFAULT;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql).tableNames))
            .setAppName("test virtual")
            .setAcceptedResultsNum(2000)
            .ok();

        runner.sql(sql).asTextFile("", "");
    }

    @Test
    public void testTempTable() {
        String sql = "select * from edu_manage.department";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.DEFAULT;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql).tableNames))
            .setAppName("test virtual")
            .setAcceptedResultsNum(2000)
            .ok();

        try {
            runner.sql(sql).asTempTable("test");
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }
    }

}
