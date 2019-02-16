package com.qihoo.qsql.codegen;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.codegen.spark.SparkBodyWrapper;
import com.qihoo.qsql.exec.AbstractPipeline;
import com.qihoo.qsql.exec.spark.SparkPipeline;
import com.qihoo.qsql.plan.QueryProcedureProducer;
import com.qihoo.qsql.plan.proc.EmbeddedElasticsearchPolicy;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.utils.SqlUtil;
import java.util.List;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class QueryGeneratorTest {
    @ClassRule
    public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

    @Test
    public void testMysqlGenerator() {
        String sql = "select * from edu_manage.department";
        List<String> tableList = SqlUtil.parseTableName(sql);
        QueryProcedureProducer producer = new QueryProcedureProducer(
            SqlUtil.getSchemaPath(tableList));
        QueryProcedure procedure = producer.createQueryProcedure(sql);

        IntegratedQueryWrapper wrapper = new SparkBodyWrapper();
        wrapper.interpretProcedure(procedure);
        wrapper.importSpecificDependency();

        Class requirementClass = wrapper.compile();
        MatcherAssert.assertThat("", requirementClass.getSuperclass().toString(),
            CoreMatchers.containsString("class com.qihoo.qsql.exec.spark.SparkRequirement"));
    }

    // @Test
    // public void testElasticsearchGenerator() {
    //     String sql = "select * from student_profile.student";
    //     AbstractPipeline pipeline = SqlRunner.builder().setTransformRunner(RunnerType.SPARK).ok().sql(sql);
    //     Assert.assertTrue(((SparkPipeline) pipeline)
    //         .source()
    //         .contains("JavaEsSparkSQL"));
    // }

    @Test
    public void testHiveGenerator() {
        String sql = "select * from action_required.homework_content";
        List<String> tableList = SqlUtil.parseTableName(sql);
        QueryProcedureProducer producer = new QueryProcedureProducer(
            SqlUtil.getSchemaPath(tableList));
        QueryProcedure procedure = producer.createQueryProcedure(sql);

        IntegratedQueryWrapper wrapper = new SparkBodyWrapper();
        wrapper.interpretProcedure(procedure);
        wrapper.importSpecificDependency();
        Class requirementClass = wrapper.compile();

        MatcherAssert.assertThat("",
            requirementClass.getSuperclass().toString(),
            CoreMatchers
                .containsString("class com.qihoo.qsql.exec.spark.SparkRequirement"));
    }

    @Test
    public void testSameDataSourceQueryGenerator() {
        String sql = "SELECT * FROM department AS DEP "
            + "INNER JOIN (SELECT * FROM student "
            + "WHERE city in ('FRAMINGHAM', 'BROCKTON', 'CONCORD')) FILTERED "
            + "ON DEP.type = FILTERED.city";
        List<String> tableList = SqlUtil.parseTableName(sql);
        QueryProcedureProducer producer = new QueryProcedureProducer(
                SqlUtil.getSchemaPath(tableList));
        QueryProcedure procedure = producer.createQueryProcedure(sql);
        IntegratedQueryWrapper wrapper = new SparkBodyWrapper();
        wrapper.interpretProcedure(procedure);
        wrapper.importSpecificDependency();
        wrapper.compile();
    }

    @Test
    public void testVirtualGenerator() {
        AbstractPipeline pipeline = SqlRunner.builder().setTransformRunner(RunnerType.SPARK).ok().sql("select 1");
        Assert.assertTrue(((SparkPipeline) pipeline).source().contains("tmp = spark.sql(\"select 1\")"));
    }
}
