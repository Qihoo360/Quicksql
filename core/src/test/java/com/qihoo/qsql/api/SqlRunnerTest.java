package com.qihoo.qsql.api;

import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.exec.JdbcPipeline;
import com.qihoo.qsql.exec.flink.FlinkPipeline;
import com.qihoo.qsql.exec.spark.SparkPipeline;
import com.qihoo.qsql.plan.proc.EmbeddedElasticsearchPolicy;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;


/**
 * Tests for {@link SqlRunner} and its child {@link DynamicSqlRunner}.
 */
public class SqlRunnerTest {
    @ClassRule
    public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

    @Test
    public void testSingleQueryForMySql() {
        String sql = "SELECT dep_id FROM edu_manage.department";
        Assert.assertEquals(buildDynamicSqlRunner().sql(sql).getClass(), JdbcPipeline.class);
    }

    @Test
    public void testSingleQueryForElasticsearch() {
        String sql = "SELECT stu_id FROM student_profile.student";
        Assert.assertEquals(buildDynamicSqlRunner().sql(sql).getClass(), JdbcPipeline.class);
    }

    @Test
    public void testSingleQueryForHive() {
        String sql = "SELECT 1";
        Assert.assertEquals(buildSparkSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testSingleQueryWithoutTableName() {
        String sql = "SELECT 1";
        Assert.assertEquals(buildDynamicSqlRunner().sql(sql).getClass(), JdbcPipeline.class);
    }

    @Test
    public void testMixQueryBetweenMySqlAndHive() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, action_required.homework_content AS b"
            + " WHERE a.dep_id = b.stu_id";
        Assert.assertEquals(buildDynamicSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testMixQueryBetweenMySqlAndElasticsearch() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, student_profile.student AS b"
            + " WHERE a.dep_id = b.stu_id";
        Assert.assertEquals(buildDynamicSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testMixQueryBetweenElasticsearchAndHive() {
        String sql = "SELECT a.stu_id, b.stu_id"
            + " FROM student_profile.student AS a, action_required.homework_content AS b"
            + " WHERE a.stu_id = b.stu_id";
        Assert.assertEquals(buildDynamicSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testMixQueryInThreeEngine() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, action_required.homework_content AS b,"
            + " student_profile.student AS c"
            + " WHERE a.dep_id = b.stu_id and b.stu_id = c.stu_id";
        Assert.assertEquals(buildDynamicSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testSingleQueryForMySqlWithSpark() {
        String sql = "SELECT dep_id FROM edu_manage.department";
        Assert.assertEquals(buildSparkSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testSingleQueryForElasticsearchWithSpark() {
        String sql = "SELECT stu_id FROM student_profile.student";
        Assert.assertEquals(buildSparkSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testSingleQueryForHiveWithSpark() {
        String sql = "SELECT stu_id FROM action_required.homework_content";
        Assert.assertEquals(buildSparkSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testSingleQueryWithoutTableNameWithSpark() {
        String sql = "SELECT 1";
        Assert.assertEquals(buildSparkSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testMixQueryBetweenMySqlAndHiveWithSpark() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, action_required.homework_content AS b"
            + " WHERE a.dep_id = b.stu_id";
        Assert.assertEquals(buildSparkSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testMixQueryBetweenMySqlAndElasticsearchWithSpark() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, student_profile.student AS b"
            + " WHERE a.dep_id = b.stu_id";
        Assert.assertEquals(buildSparkSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testMixQueryBetweenElasticsearchAndHiveWithSpark() {
        String sql = "SELECT a.stu_id, b.stu_id"
            + " FROM student_profile.student AS a, action_required.homework_content AS b"
            + " WHERE a.stu_id = b.stu_id";
        Assert.assertEquals(buildSparkSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testMixQueryInThreeEngineWithSpark() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, action_required.homework_content AS b,"
            + " student_profile.student AS c"
            + " WHERE a.dep_id = b.stu_id and b.stu_id = c.stu_id";
        Assert.assertEquals(buildSparkSqlRunner().sql(sql).getClass(), SparkPipeline.class);
    }

    @Test
    public void testMixQueryBetweenMySqlAndHiveWithFlink() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, action_required.homework_content AS b"
            + " WHERE a.dep_id = b.stu_id";
        Assert.assertEquals(buildFlinkSqlRunner().sql(sql).getClass(), FlinkPipeline.class);
    }

    @Test
    public void testMixQueryBetweenMySqlAndElasticsearchWithFlink() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, student_profile.student AS b"
            + " WHERE a.dep_id = b.stu_id";
        Assert.assertEquals(buildFlinkSqlRunner().sql(sql).getClass(), FlinkPipeline.class);
    }

    @Test
    public void testMixQueryBetweenElasticsearchAndHiveWithFlink() {
        String sql = "SELECT a.stu_id, b.stu_id"
            + " FROM student_profile.student AS a, action_required.homework_content AS b"
            + " WHERE a.stu_id = b.stu_id";
        Assert.assertEquals(buildFlinkSqlRunner().sql(sql).getClass(), FlinkPipeline.class);
    }

    @Test
    public void testMixQueryInThreeEngineWithFlink() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, action_required.homework_content AS b,"
            + " student_profile.student AS c"
            + " WHERE a.dep_id = b.stu_id and b.stu_id = c.stu_id";
        Assert.assertEquals(buildFlinkSqlRunner().sql(sql).getClass(), FlinkPipeline.class);
    }

    @Test
    public void testInsertOutput() {
        String sql = "INSERT INTO `hello` IN HDFS SELECT 1";
        buildSparkSqlRunner().sql(sql).show();
    }

    private SqlRunner buildDynamicSqlRunner() {
        return SqlRunner.builder()
            .setTransformRunner(SqlRunner.Builder.RunnerType.DEFAULT)
            .setAppName("Test for DynamicSqlRunner")
            .setAcceptedResultsNum(20)
            .ok();
    }

    private SqlRunner buildSparkSqlRunner() {
        return SqlRunner.builder()
            .setTransformRunner(RunnerType.SPARK)
            .setAppName("Test for DynamicSqlRunner")
            .setAcceptedResultsNum(- 1)
            .ok();
    }

    private SqlRunner buildFlinkSqlRunner() {
        return SqlRunner.builder()
            .setTransformRunner(RunnerType.FLINK)
            .setAppName("Test for DynamicSqlRunner")
            .setAcceptedResultsNum(20)
            .ok();
    }

}
