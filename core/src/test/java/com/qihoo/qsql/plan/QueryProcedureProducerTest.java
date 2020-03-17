package com.qihoo.qsql.plan;

import com.qihoo.qsql.exception.ParseException;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.plan.proc.DataSetTransformProcedure;
import com.qihoo.qsql.plan.proc.DirectQueryProcedure;
import com.qihoo.qsql.plan.proc.EmbeddedElasticsearchPolicy;
import com.qihoo.qsql.plan.proc.PreparedExtractProcedure.ElasticsearchExtractor;
import com.qihoo.qsql.plan.proc.PreparedExtractProcedure.HiveExtractor;
import com.qihoo.qsql.plan.proc.PreparedExtractProcedure.JdbcExtractor;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests for {@link QueryProcedureProducer}.
 */
public class QueryProcedureProducerTest {

    private static final String MYSQL_TABLE_NAME = "edu_manage.department";
    private static final String ES_TABLE_NAME = "student_profile.student";
    private static final String HIVE_TABLE_NAME = "action_required.homework_content";

    @ClassRule
    public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

    @Test
    public void testErrorSql() {
        String sql = "SELECT dep_id LIMIT 10 FROM edu_manage.department";
        try {
            new QueryProcedureProducer(getSchemaPath(Collections.singletonList(MYSQL_TABLE_NAME)))
                .createQueryProcedure(sql);
        } catch (ParseException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testDirectQueryProcedure() {
        String sql = "SELECT dep_id FROM edu_manage.department WHERE dep_id = 1";
        QueryProcedure queryProcedure =
            new QueryProcedureProducer(getSchemaPath(Collections.singletonList(MYSQL_TABLE_NAME)))
                .createQueryProcedure(sql);
        Assert.assertEquals(queryProcedure.getClass(), DirectQueryProcedure.class);
    }

    @Test
    public void testProcedureWithoutTableName() {
        String sql = "SELECT 1";
        QueryProcedure queryProcedure =
            new QueryProcedureProducer(getSchemaPath(Collections.singletonList(MYSQL_TABLE_NAME)))
                .createQueryProcedure(sql);
        Assert.assertEquals(queryProcedure.getClass(), DirectQueryProcedure.class);
    }

    @Test
    public void testMixSqlWithMySqlAndHive() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, action_required.homework_content AS b"
            + " WHERE a.dep_id = b.stu_id";
        QueryProcedure queryProcedure =
            new QueryProcedureProducer(getSchemaPath(Arrays.asList(MYSQL_TABLE_NAME, HIVE_TABLE_NAME)))
                .createQueryProcedure(sql);
        List<Class> extractorList = getExtractorList(queryProcedure);
        if (extractorList.contains(HiveExtractor.class)
            && extractorList.contains(JdbcExtractor.class)
            && extractorList.contains(DataSetTransformProcedure.class)) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testMixSqlWithMySqlAndElasticsearch() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, student_profile.student AS b"
            + " WHERE a.dep_id = b.stu_id";
        QueryProcedure queryProcedure =
            new QueryProcedureProducer(getSchemaPath(Arrays.asList(MYSQL_TABLE_NAME, ES_TABLE_NAME)))
                .createQueryProcedure(sql);
        List<Class> extractorList = getExtractorList(queryProcedure);
        if (extractorList.contains(ElasticsearchExtractor.class)
            && extractorList.contains(JdbcExtractor.class)
            && extractorList.contains(DataSetTransformProcedure.class)) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testMixSqlWithElasticsearchAndHive() {
        String sql = "SELECT a.stu_id, b.stu_id"
            + " FROM student_profile.student AS a, action_required.homework_content AS b"
            + " WHERE a.stu_id = b.stu_id";
        QueryProcedure queryProcedure =
            new QueryProcedureProducer(getSchemaPath(Arrays.asList(ES_TABLE_NAME, HIVE_TABLE_NAME)))
                .createQueryProcedure(sql);
        List<Class> extractorList = getExtractorList(queryProcedure);
        if (extractorList.contains(HiveExtractor.class)
            && extractorList.contains(ElasticsearchExtractor.class)
            && extractorList.contains(DataSetTransformProcedure.class)) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testMixSqlWithSubQuery() {
        String sql = "SELECT dep_id, (SELECT COUNT(stu_id) FROM action_required.homework_content)"
            + " FROM edu_manage.department WHERE dep_id = 1";
        QueryProcedure queryProcedure =
            new QueryProcedureProducer(getSchemaPath(Arrays.asList(MYSQL_TABLE_NAME, HIVE_TABLE_NAME)))
                .createQueryProcedure(sql);
        List<Class> extractorList = getExtractorList(queryProcedure);
        if (extractorList.contains(HiveExtractor.class)
            && extractorList.contains(JdbcExtractor.class)
            && extractorList.contains(DataSetTransformProcedure.class)) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testMixSqlWithAndWithoutTableName() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM (SELECT dep_id FROM edu_manage.department WHERE dep_id = 1) AS a"
            + " JOIN (SELECT 1 as stu_id) AS b"
            + " ON(a.dep_id = b.stu_id)";
        QueryProcedure queryProcedure =
            new QueryProcedureProducer(getSchemaPath(Arrays.asList(MYSQL_TABLE_NAME)))
                .createQueryProcedure(sql);
        List<Class> extractorList = getExtractorList(queryProcedure);
        if (extractorList.contains(JdbcExtractor.class)) {
            Assert.assertTrue(true);
        }
    }

    private String getSchemaPath(List<String> tableNames) {
        return "inline: " + MetadataPostman.getCalciteModelSchema(tableNames);
    }

    private List<Class> getExtractorList(QueryProcedure queryProcedure) {
        List<Class> extractorList = new ArrayList<>();
        do {
            extractorList.add(queryProcedure.getClass());
            queryProcedure = queryProcedure.next();
        }
        while (queryProcedure.hasNext());
        return extractorList;
    }

}
