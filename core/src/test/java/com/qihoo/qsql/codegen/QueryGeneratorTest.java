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
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class QueryGeneratorTest {
    @ClassRule
    public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

    @Test
    public void testHiveGenerator() {
        assertGenerateClass("select * from action_required.homework_content");
    }

    @Test
    public void testVirtualGenerator() {
        AbstractPipeline pipeline = SqlRunner.builder().setTransformRunner(RunnerType.SPARK).ok().sql("select 1");
        System.out.println(((SparkPipeline) pipeline).source());
        Assert.assertTrue(((SparkPipeline) pipeline).source().contains("spark.sql(\"SELECT 1\")"));
    }

    @Test
    public void testMysqlRegexpExtract() {
        assertGenerateClass("SELECT REGEXP_EXTRACT(type, '.*', 0) FROM department",
            "spark.read().jdbc(\"\", \"(select dep_id, cycle, type, times from edu_manage.department) "
                + "edu_manage_department_0\", "
                + "SparkJdbcGenerator.config(\"username\", \"password\", \"\"))",
            "createOrReplaceTempView(\"edu_manage_department_0\")",
            "String sql = \"SELECT REGEXP_EXTRACT(type, '.*', 0) AS expr_col__0 FROM edu_manage_department_0\"",
            "spark.sql(sql)");
    }

    private void assertGenerateClass(String sql, String...args) {
        List<String> tableList = SqlUtil.parseTableName(sql).tableNames;
        QueryProcedureProducer producer = new QueryProcedureProducer(
            SqlUtil.getSchemaPath(tableList), SqlRunner.builder());
        QueryProcedure procedure = producer.createQueryProcedure(sql);

        SparkBodyWrapper wrapper = new SparkBodyWrapper();
        wrapper.interpretProcedure(procedure);
        wrapper.importSpecificDependency();
        wrapper.compile();
        String clazz = wrapper.toString();
        System.out.println(clazz);
        Assert.assertTrue(Arrays.stream(args).allMatch(clazz::contains));
    }
}
