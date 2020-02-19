package com.qihoo.qsql.plan.proc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.plan.QueryProcedureProducer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class QueryProcedureTest {

    @ClassRule
    public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

    private static QueryProcedureProducer producer;

    private static List<String> tableNames = Arrays.asList(
        "edu_manage.department",
        "student_profile.student",
        "edu_manage.department_student_relation",
        "action_required.homework_content",
        "action_required.action_detection_in_class");

    /**
     * test.
     *
     * @throws IOException io exception
     */

    @BeforeClass
    public static void setupInstance() throws IOException {
        final Map<String, String> mapping = ImmutableMap.of("stu_id", "long", "type", "long",
            "city", "keyword", "digest", "long", "province", "keyword");
        NODE.createIndex("student", mapping);

        // load records from file
        final List<ObjectNode> bulk = new ArrayList<>();
        Resources.readLines(QueryProcedureTest.class.getResource("/student.json"),
            StandardCharsets.UTF_8, new LineProcessor<Void>() {
                @Override
                public boolean processLine(String line) throws IOException {
                    line = line.replaceAll("_id", "id");
                    bulk.add((ObjectNode) NODE.mapper().readTree(line));
                    return true;
                }

                @Override
                public Void getResult() {
                    return null;
                }
            });

        if (bulk.isEmpty()) {
            throw new IllegalStateException("No records to index. Empty file ?");
        }

        NODE.insertBulk("student", bulk);

        producer = new QueryProcedureProducer(
            "inline: " + MetadataPostman.getCalciteModelSchema(tableNames), SqlRunner.builder());
    }

    @Test
    public void testOnlyValue() {
        String sql = "SELECT 'Hello World' AS col1, 1010 AS col2";
        prepareForChecking(sql).checkExtra("SELECT 'Hello World' AS `col1`, 1010 AS `col2`");
    }

    @Test
    public void testWith() {
        String sql = "WITH department AS \n"
            + "    (SELECT times\n"
            + "    FROM department\n"
            + "    GROUP BY  times), student AS \n"
            + "    (SELECT *\n"
            + "    FROM student LIMIT 30) \n"
            + "    (SELECT *\n"
            + "    FROM department\n"
            + "    INNER JOIN student\n"
            + "        ON department.times = student.city LIMIT 10)";
        prepareForChecking(sql)
            .checkExtra("{\"_source\":[\"city\",\"province\",\"digest\",\"type\",\"stu_id\"],\"size\":30}",
            "select times from edu_manage.department group by times")
            .checkTrans("SELECT edu_manage_department_1.times, t.city, t.province, t.digest, t.type, t.stu_id "
                + "FROM edu_manage_department_1 INNER JOIN (SELECT city, province, digest, type, stu_id, "
                + "CAST(city AS INTEGER) AS city0 FROM student_profile_student_0) "
                + "AS t ON edu_manage_department_1.times = t.city0 LIMIT 10");
    }

    @Test
    public void testElasticsearchEmbeddedSource() {
        String sql = "SELECT city FROM student";
        prepareForChecking(sql).checkExtra("{\"_source\":[\"city\"]}");
    }

    @Test
    public void testQuoting() {
        String sql = "SELECT \"double-quote\", 'quote', times as `c` FROM department as `dep`";
        prepareForChecking(sql).checkExtra("select 'double-quote' as expr_idx_0, 'quote' as expr_idx_1, times as c "
            + "from edu_manage.department");
    }

    @Test
    public void testScalarQueryWithGroupBy() {
        String sql =
            "SELECT * FROM department AS dep INNER JOIN edu_manage.department_student_relation rel "
                + "ON dep.dep_id = rel.stu_id "
                + "WHERE EXISTS (SELECT 1 FROM department GROUP BY type)";
        prepareForChecking(sql).checkExtra(
            "select department.dep_id, department.cycle, department.type, department.times,"
                + " department_student_relation.id, department_student_relation.dep_id as dep_id0,"
                + " department_student_relation.stu_id "
                + "from edu_manage.department inner join edu_manage.department_student_relation "
                + "on department.dep_id = department_student_relation.stu_id, "
                + "(select true as i from edu_manage.department group by true) as t2");
    }

    @Test
    public void testCorrelationForProject() {
        String sql = "SELECT MIN(digest), MAX(digest), type "
            + "FROM student_profile.student group by type order by "
            + "type limit 3";
        //prepareForChecking(sql).checkExtra("");
    }

    @Test
    public void testValueIn() {
        String sql = "SELECT UPPER('Time') NOT IN ('Time', 'New', 'Roman') AS res";
        prepareForChecking(sql)
            .checkExtra("SELECT UPPER('Time') NOT IN ('Time', 'New', 'Roman') AS `res`");
    }

    @Test
    public void testDatePlusInteger() {
        String sql = "SELECT * FROM action_required.homework_content a join action_required.homework_content b"
            + " on a.stu_id = b.stu_id  where a.date_time > b.date_time + 1";
        prepareForChecking(sql).checkExtra(
            "SELECT homework_content.stu_id, homework_content.date_time, homework_content.signature, "
                + "homework_content.course_type, homework_content.content, homework_content0.stu_id stu_id0, "
                + "homework_content0.date_time date_time0, homework_content0.signature signature0, "
                + "homework_content0.course_type course_type0, homework_content0.content content0 "
                + "FROM action_required.homework_content INNER JOIN action_required.homework_content homework_content0 "
                + "ON homework_content.stu_id = homework_content0.stu_id "
                + "AND homework_content.date_time > (homework_content0.date_time + 1)");
    }


    @Test
    public void testValueWithUselessTableScan() {
        String sql = "SELECT 1 IN (SELECT dep.times FROM edu_manage.department AS dep) AS res";
        prepareForChecking(sql).checkExtra("SELECT 1 IN (SELECT `dep`.`times` "
            + "FROM `edu_manage`.`department` AS `dep`) AS `res`");
    }

    @Test
    public void testFilterAnd() {
        String sql = "SELECT dep.type FROM edu_manage.department AS dep "
            + "WHERE dep.times BETWEEN 10 AND 20 AND (dep.type LIKE '%abc%')";
        prepareForChecking(sql).checkExtra(
            "select type from edu_manage.department where times >= 10 and times <= 20 and type like '%abc%'");
    }

    @Test
    public void testSelectWithoutFrom() {
        prepareForChecking("SELECT 1").checkExtra(
            "SELECT 1");
        prepareForChecking("SELECT 'hello' < SOME ('world', 'hi')").checkExtra(
            "SELECT 'hello' < SOME ('world', 'hi')");
    }

    @Test
    public void testSelectWithoutFromWithJoin() {
        String sql = "SELECT a.e1 FROM (SELECT 1 e1) as a join (SELECT 2 e2) as b ON (a.e1 = b.e2)";
        prepareForChecking(sql).checkExtra(
            "SELECT `a`.`e1` FROM (SELECT 1 AS `e1`) AS `a` "
                + "INNER JOIN (SELECT 2 AS `e2`) AS `b` ON `a`.`e1` = `b`.`e2`");
    }

    @Test
    public void testSimpleArithmetic() {
        String sql = "SELECT ABS(-1) + FLOOR(1.23) % 1 AS res";
        prepareForChecking(sql).checkExtra("SELECT ABS(-1) + FLOOR(1.23) % 1 AS `res`");
    }

    @Test
    public void testFunctionLength() {
        //original function is length
        String sql = "SELECT ABS(CHAR_LENGTH('Hello World')) AS res";
        prepareForChecking(sql).checkExtra("SELECT ABS(CHAR_LENGTH('Hello World')) AS `res`");
    }

    @Test
    //TODO resolve subquery function control
    public void testFunctionConcat() {
        String sql = "SELECT SUBSTRING('Hello World', 0, 5) || SUBSTRING('Hello World', 5) AS res";
        prepareForChecking(sql)
            .checkExtra("SELECT SUBSTRING('Hello World', 0, 5) || SUBSTRING('Hello World', 5) AS `res`");
    }

    @Test
    public void testTypeBoolean() {
        String sql = "SELECT TRUE, NOT TRUE, 1 IS NOT NULL";
        prepareForChecking(sql).checkExtra("SELECT TRUE, NOT TRUE, 1 IS NOT NULL");
    }

    @Test
    public void testComparison() {
        String sql = "SELECT (1 < 2 <> TRUE) AND TRUE AS res";
        prepareForChecking(sql).checkExtra("SELECT 1 < 2 <> TRUE AND TRUE AS `res`");
    }

    @Test
    public void testValueWithIn() {
        String sql = "SELECT UPPER('Time') NOT IN ('Time', 'New', 'Roman') AS res";
        prepareForChecking(sql)
            .checkExtra("SELECT UPPER('Time') NOT IN ('Time', 'New', 'Roman') AS `res`");
    }

    @Test
    public void testSelectWithExistsScalarQuery() {
        String sql = "SELECT EXISTS (SELECT 1) "
            + "FROM edu_manage.department AS test "
            + "WHERE test.type IN ('male', 'female')";
        prepareForChecking(sql).checkExtra("select t1.i is not null as expr_idx_0 "
            + "from (select * from edu_manage.department "
            + "where type = 'male' or type = 'female') as t left join "
            + "(select true as i) as t1 on true");
    }

    @Test
    public void testSelectArithmetic() {
        //es not support arithmetic
        String sql = "SELECT (digest * 2 - 5) AS res \n"
            + "FROM student_profile.student LIMIT 10";
        try {
            prepareForChecking(sql).checkExtra("");
        } catch (IllegalArgumentException ex) {
            Assert.assertTrue(true);
        }
    }

    //Bug
    @Test
    public void testAggregationsByElastic() {
        String sql = "SELECT MIN(digest), MAX(digest), type "
            + "FROM student_profile.student group by type order by "
            + "type limit 3";
        //prepareForChecking(sql).checkExtra("");
    }

    @Test
    public void testTreeDivision() {
        //TODO This case failed due to the addition of the following two rules,
        //Failed grammar: case when
        //Rules caused the failure: JoinConditionPushRule.FILTER_ON_JOIN,JoinConditionPushRule.JOIN
        String sql = "SELECT type, times\n"
            + "FROM edu_manage.department\n"
            + "WHERE dep_id < SOME(\n"
            + "SELECT stu_id \n"
            + "FROM action_required.homework_content \n"
            + "ORDER BY stu_id LIMIT 100) AND times > ALL(10, 11, 12)";
        prepareForChecking(sql)
            .checkExtra("SELECT MAX(stu_id) m, COUNT(*) c,"
                    + " COUNT(stu_id) d FROM (SELECT stu_id FROM action_required.homework_content "
                    + "ORDER BY stu_id LIMIT 100) t1",
                "select dep_id, cycle, type, times from edu_manage.department where times > 12")
            // .checkTrans("SELECT edu_manage_department_0.type,"
            //     + " edu_manage_department_0.times FROM edu_manage_department_0,"
            //     + " action_required_homework_content_1 WHERE CASE WHEN action_required_homework_content_1.c = 0 "
            //     + "THEN FALSE WHEN edu_manage_department_0.dep_id <"
            //     + " action_required_homework_content_1.m IS TRUE "
            //     + "THEN TRUE WHEN action_required_homework_content_1.c > action_required_homework_content_1.d "
            //     + "THEN NULL ELSE edu_manage_department_0.dep_id < action_required_homework_content_1.m END "
            //     + "AND edu_manage_department_0.times > 12")
            .checkArchitect("[E]->[E]->[T]->[L]");
    }

    @Test
    public void testGroupByWithHaving() {
        //es not support having
        String sql = "SELECT count(digest), city \n"
            + "FROM student_profile.student \n"
            + "GROUP BY (ip) "
            + "HAVING LEFT(ip, 3) LIKE '10%'\n"
            + "ORDER BY 1";
        //prepareForChecking(sql).checkExtra("");
    }

    @Test
    public void testComplexGroupByWithHaving() {
        String sql = "SELECT course_type \n"
            + "FROM action_required.homework_content AS test\n"
            + "WHERE date_time = '20180820'\n"
            + "GROUP BY course_type\n"
            + "HAVING ((COUNT(*) > 100) AND (1 = 2))\n"
            + "ORDER BY course_type";
        prepareForChecking(sql).checkExtra("SELECT `test`.`course_type` "
            + "FROM `action_required`.`homework_content` AS `test` "
            + "WHERE `test`.`date_time` = '20180820' "
            + "GROUP BY `test`.`course_type` HAVING COUNT(*) > 100 AND 1 = 2 "
            + "ORDER BY `course_type` ORDER BY `course_type`");
    }

    @Test
    public void testScalarSubQueryWithIn() {
        String sql = "SELECT test1.signature \n"
            + "FROM action_required.homework_content AS test1\n"
            + "LEFT OUTER JOIN action_required.homework_content AS test2\n"
            + "ON test1.content = test2.signature \n"
            + "WHERE test2.content IS NULL AND test1.date_time IN(SELECT '20180713')";
        prepareForChecking(sql)
            .checkExtra("SELECT t.signature FROM (SELECT * FROM action_required.homework_content "
                + "LEFT JOIN action_required.homework_content homework_content0 "
                + "ON homework_content.content = homework_content0.signature "
                + "WHERE homework_content0.content IS NULL) t "
                + "INNER JOIN (SELECT '20180713' expr_idx_0) t1 "
                + "ON t.date_time = t1.expr_idx_0");
    }

    @Test
    public void testComplexSingleValue() {
        String sql = "SELECT (SELECT (SELECT 1))";
        prepareForChecking(sql).checkExtra("SELECT (((SELECT (((SELECT 1))))))");
    }

    @Test
    public void testConcatValue() {
        String sql = "SELECT 'a' || 'b'";
        prepareForChecking(sql).checkExtra("SELECT 'a' || 'b'");
    }

    @Test
    public void testJoinConditionWithUsing() {
        String sql = "SELECT count(*)\n"
            + "FROM action_required.homework_content AS test1\n"
            + "LEFT OUTER JOIN action_required.homework_content AS test2 \n"
            + "USING(stu_id) \n"
            + "GROUP BY test1.date_time, test2.date_time\n"
            + "HAVING test1.date_time > '20180713' AND test2.date_time > '20180731'";
        prepareForChecking(sql).checkExtra("SELECT COUNT(*) expr_idx_0 "
            + "FROM action_required.homework_content LEFT JOIN "
            + "action_required.homework_content homework_content0 "
            + "ON homework_content.stu_id = homework_content0.stu_id "
            + "GROUP BY homework_content.date_time, homework_content0.date_time "
            + "HAVING homework_content.date_time > '20180713' AND homework_content0.date_time >"
            + " '20180731'").checkArchitect("[D]->[E]->[T]->[L]");
    }

    @Test
    public void testMixedSqlConcatAndSome() {
        //TODO This case failed due to the addition of the following two rules,
        //Failed grammar: case when
        //Rules caused the failure: JoinConditionPushRule.FILTER_ON_JOIN,JoinConditionPushRule.JOIN
        String sql = "SELECT TRIM(es.city) || TRIM(msql.course_type)\n"
            + "FROM student_profile.student AS es\n"
            + "INNER JOIN\n"
            + "action_required.homework_content as msql\n"
            + "ON es.stu_id = msql.stu_id\n"
            + "WHERE es.digest > Some(\n"
            + "\tSELECT times FROM edu_manage.department)";
        prepareForChecking(sql)
            .checkExtra(
                "SELECT stu_id, date_time, signature, course_type, content FROM action_required.homework_content",
                "select min(times) as m, count(*) as c, count(times) as d from edu_manage.department",
                "{\"_source\":[\"city\",\"province\",\"digest\",\"type\",\"stu_id\"]}");
        // .checkTrans("SELECT CONCAT(TRIM(student_profile_student_0.city),"
        //     + " TRIM(action_required_homework_content_1.course_type)) AS expr_idx_0"
        //     + " FROM student_profile_student_0 INNER JOIN action_required_homework_content_1"
        //     + " ON student_profile_student_0.stu_id = action_required_homework_content_1.stu_id,"
        //     + " edu_manage_department_2 WHERE CASE WHEN edu_manage_department_2.c = 0"
        //     + " THEN FALSE WHEN student_profile_student_0.digest > edu_manage_department_2.m IS TRUE"
        //     + " THEN TRUE WHEN edu_manage_department_2.c > edu_manage_department_2.d"
        //     + " THEN NULL ELSE student_profile_student_0.digest > edu_manage_department_2.m END");

    }

    @Test
    public void testMixedDataTimeAndReverse() {
        //TODO resolve month to extract dialect problem
        String sql = "SELECT * FROM\n"
            + "(SELECT signature AS reved, CURRENT_TIMESTAMP AS pmonth\n"
            + "\tFROM action_required.homework_content \n"
            + "\tORDER BY date_time) AS hve \n"
            + "\tFULL JOIN\n"
            + "(SELECT TRIM(type), '20180101' AS pday\n"
            + "\tFROM edu_manage.department) AS msql\n"
            + "\tON hve.pmonth = msql.pday";
        prepareForChecking(sql)
            .checkExtra("SELECT signature reved, CURRENT_TIMESTAMP pmonth "
                    + "FROM action_required.homework_content",
                "select trim(type) as expr_idx_0, '20180101' as pday from edu_manage.department")
            .checkTrans("SELECT action_required_homework_content_0.reved, "
                + "action_required_homework_content_0.pmonth, "
                + "edu_manage_department_1.expr_idx_0, edu_manage_department_1.pday "
                + "FROM action_required_homework_content_0 "
                + "FULL JOIN edu_manage_department_1 "
                + "ON action_required_homework_content_0.pmonth = edu_manage_department_1.pday")
            .checkArchitect("[E]->[E]->[T]->[L]");
    }

    @Test
    public void testMixedIfAndElasticsearchLike() {
        String sql = "SELECT (SELECT MAX(digest)\n"
            + "\tFROM student_profile.student \n"
            + "\tWHERE province = 'hunan'), \n"
            + "CASE hve.signature WHEN 'abc' THEN 'cde' \n"
            + "ELSE 'def' END, (CASE WHEN hve.date_time <> '20180820' "
            + "THEN 'Hello' ELSE 'WORLD' END) AS col\n"
            + "FROM action_required.homework_content AS hve\n"
            + "WHERE hve.date_time BETWEEN '20180810' AND '20180830'";
        prepareForChecking(sql)
            .checkExtra(
                "SELECT stu_id, date_time, signature, course_type, content "
                    + "FROM action_required.homework_content "
                    + "WHERE date_time >= '20180810' AND date_time <= '20180830'",
                "{\"query\":{\"constant_score\":{\"filter\":"
                    + "{\"term\":{\"province\":\"hunan\"}}}},\"_source\":[\"digest\"]}")
            .checkTrans("SELECT t.expr_idx_0, CASE WHEN action_required_homework_content_1.signature = 'abc' "
                + "THEN 'cde' ELSE 'def' END AS expr_idx_1, "
                + "CASE WHEN action_required_homework_content_1.date_time <> '20180820' "
                + "THEN 'Hello' ELSE 'WORLD' END AS col FROM action_required_homework_content_1 "
                + "LEFT JOIN (SELECT MAX(digest) AS expr_idx_0 FROM student_profile_student_0) AS t ON TRUE")
            .checkArchitect(("[E]->[E]->[T]->[L]"));
    }

    @Test
    public void testMixedGroupBy() {
        //not support concat
        String sql = "SELECT MOD(es1.age, 10) + FLOOR(es2.age)\n"
            + "FROM (SELECT ip, age, count(*)\n"
            + "\tFROM profile \n"
            + "\tGROUP BY ip, age \n"
            + "\tLIMIT 100) AS es1, (\n"
            + "\tSELECT ip, country, age\n"
            + "\tFROM profile as tmp \n"
            + "\tWHERE tmp.age > 10) AS es2 \n"
            + "\tWHERE es1.ip = es2.ip \n"
            + "\tAND es2.country IS NOT NULL";
    }


    @Test
    public void testElasticsearchGroupBy() {
        String sql = "select count(city), province from student_profile.student "
            + "group by province order by province limit 10";
        prepareForChecking(sql, RunnerType.DEFAULT)
            .checkExtra("{\"_source\":[\"province\",\"city\"]}")
            .checkTrans("SELECT COUNT(*) AS expr_idx_0, province "
                + "FROM student_profile_student_0 GROUP BY province ORDER BY province LIMIT 10")
            .checkArchitect("[D]->[E]->[T]->[L]");

    }

    @Test
    public void testMixedSqlComplexQuery() {
        //not support if
        String sql = "(SELECT msql.mids IN (SELECT 'hello' \n"
            + "\tFROM student_profile.student \n"
            + "\tWHERE type IN ('scholar', 'master')\n"
            + "\tAND city = 'here') AS encode_flag, \n"
            + " \ttrue AS action_flag\n"
            + "FROM mysql_daily AS msql)\n"
            + "UNION\n"
            + "(SELECT false, false \n"
            + "\tFROM hive_daily AS hve\n"
            + "\tGROUP BY pday\n"
            + "\tHAVING pday > 20180810)";

    }

    @Test
    public void testMixedSqlSubQuery() {
        String sql = "(SELECT COUNT(date_time), COUNT(signature) \n"
            + "\tFROM action_required.homework_content AS hive\n"
            + "WHERE date_time IN (SELECT type FROM student_profile.student) "
            + "\tGROUP BY date_time, signature)";
        prepareForChecking(sql).checkExtra(
            "SELECT stu_id, date_time, signature, course_type, content FROM action_required.homework_content",
            "{\"_source\":[\"type\"]}")
            .checkTrans("SELECT COUNT(*) AS expr_idx_0, COUNT(*) AS expr_idx_1 "
                + "FROM action_required_homework_content_1 "
                + "INNER JOIN (SELECT type FROM student_profile_student_0 GROUP BY type) AS t "
                + "ON action_required_homework_content_1.date_time = t.type "
                + "GROUP BY action_required_homework_content_1.date_time, action_required_homework_content_1.signature")
            .checkArchitect("[E]->[E]->[T]->[L]");
    }

    @Test
    public void testElasticsearchParse() {
        String sql = "(SELECT province FROM student_profile.student where "
            + "type='100054') UNION (SELECT signature FROM "
            + "action_required.homework_content where course_type = "
            + "'english')";

        prepareForChecking(sql)
            .checkExtra(
                "SELECT signature FROM action_required.homework_content WHERE course_type = 'english'",
                "{\"query\":{\"constant_score\":{\"filter\":{\"term\":{\"type\":\"100054\"}}}}"
                    + ",\"_source\":[\"province\"]}")
            .checkTrans("SELECT * FROM student_profile_student_0 UNION "
                + "SELECT * FROM action_required_homework_content_1")
            .checkArchitect("[E]->[E]->[T]->[L]");
    }

    @Test
    public void testSimpleJoin() {
        String sql =
            "SELECT * FROM (SELECT stu_id, times FROM edu_manage.department AS dep "
                + "INNER JOIN edu_manage.department_student_relation AS relation "
                + "ON dep.dep_id = relation.dep_id) AS stu_times INNER JOIN "
                + "action_required.homework_content logparse ON stu_times.stu_id = logparse.stu_id "
                + "WHERE logparse.date_time = '20180901' LIMIT 100";

        prepareForChecking(sql)
            .checkExtra("SELECT stu_id, date_time, signature, course_type, content "
                    + "FROM action_required.homework_content WHERE date_time = '20180901'",
                "select department_student_relation.stu_id, department.times "
                    + "from edu_manage.department inner join "
                    + "edu_manage.department_student_relation "
                    + "on department.dep_id = department_student_relation.dep_id")
            .checkTrans("SELECT edu_manage_department_0.stu_id, edu_manage_department_0.times,"
                + " action_required_homework_content_1.stu_id AS stu_id0, "
                + "action_required_homework_content_1.date_time, "
                + "action_required_homework_content_1.signature, "
                + "action_required_homework_content_1.course_type, "
                + "action_required_homework_content_1.content FROM edu_manage_department_0 "
                + "INNER JOIN action_required_homework_content_1 "
                + "ON edu_manage_department_0.stu_id = action_required_homework_content_1.stu_id LIMIT 100")
            .checkArchitect("[E]->[E]->[T]->[L]");
    }

    @Test
    public void testNotExistedFunctionsInFilter() {
        prepareForChecking("SELECT type FROM edu_manage.department "
            + "WHERE (' world ') = 'world' GROUP BY type, times "
            + "HAVING TRIM(' hello ') = 'hello' and CEIL(times) = 1", RunnerType.SPARK)
            .checkExtra("select type, times from edu_manage.department "
                + "where ' world ' = 'world' group by type, times")
            .checkTrans("SELECT type FROM edu_manage_department_0 "
                + "WHERE TRIM(' hello ') = 'hello' AND CEIL(times) = 1");

        //After cut having op, there is no project in sql, so returns '*'
        prepareForChecking("SELECT type FROM edu_manage.department "
            + "WHERE LENGTH(' world ') = 3 OR CEIL(times) = 1 "
            + "GROUP BY type HAVING TRIM(' hello ') = 'hello'", RunnerType.SPARK)
            .checkExtra("select dep_id, cycle, type, times from edu_manage.department")
            .checkTrans("SELECT type FROM edu_manage_department_0 WHERE LENGTH(' world ') = 3"
                + " OR CEIL(times) = 1 GROUP BY type HAVING TRIM(' hello ') = 'hello'");
    }

    @Test
    public void testCaseWhen() {
        prepareForChecking("SELECT CASE type WHEN 'a' THEN 'b' ELSE 'c' END FROM edu_manage.department")
            .checkExtra("select case when type = 'a' then 'b' else 'c' end as expr_idx_0 from edu_manage.department");
    }

    @Test
    public void testIf() {
        prepareForChecking("SELECT IF(10 > 1, 'hello', 2) FROM action_required.homework_content")
            .checkExtra("SELECT IF(TRUE, 'hello', 2) expr_idx_0 FROM action_required.homework_content");
    }

    @Test
    public void testElasticJoin() {
        prepareForChecking("SELECT * from student_profile.student as a "
            + "inner join student_profile.student as b on a.stu_id = b.stu_id")
            .checkExtra("{\"_source\":[\"city\",\"province\",\"digest\",\"type\",\"stu_id\"]}",
                "{\"_source\":[\"city\",\"province\",\"digest\",\"type\",\"stu_id\"]}")
            .checkTrans("SELECT student_profile_student_0.city, student_profile_student_0.province,"
                + " student_profile_student_0.digest, student_profile_student_0.type,"
                + " student_profile_student_0.stu_id, student_profile_student_1.city AS city0,"
                + " student_profile_student_1.province AS province0,"
                + " student_profile_student_1.digest AS digest0, student_profile_student_1.type AS type0,"
                + " student_profile_student_1.stu_id AS stu_id0 FROM student_profile_student_0"
                + " INNER JOIN student_profile_student_1"
                + " ON student_profile_student_0.stu_id = student_profile_student_1.stu_id");
    }

    @Test
    public void testRegexpOperation() {
        prepareForChecking("SELECT regexp_extract(signature, '[0-9]*', 1) FROM action_required.homework_content")
            .checkExtra("SELECT REGEXP_EXTRACT(signature, '[0-9]*', 1) expr_idx_0 "
                + "FROM action_required.homework_content");

        prepareForChecking("SELECT regexp_extract("
            + "regexp_replace(signature, '[0-9]+', 'hello'), '[0-9]*', 1) "
            + "FROM action_required.homework_content WHERE LENGTH(signature) > 10")
            .checkExtra("SELECT REGEXP_EXTRACT("
                + "REGEXP_REPLACE(signature, '[0-9]+', 'hello'), '[0-9]*', 1) expr_idx_0 "
                + "FROM action_required.homework_content WHERE LENGTH(signature) > 10");
    }

    @Test
    public void testConcatTranslate() {
        prepareForChecking("SELECT signature || 'hello' || 'world' FROM action_required.homework_content")
            .checkExtra("SELECT CONCAT(CONCAT(signature, 'hello'), 'world') expr_idx_0 "
                + "FROM action_required.homework_content");
    }

    @Test
    public void testCountDistinct() {
        prepareForChecking("SELECT count(distinct signature) res FROM action_required.homework_content")
            .checkExtra("SELECT COUNT(DISTINCT signature) res FROM action_required.homework_content");
    }

    @Test
    public void testNotExistedFunction() {
        try {
            prepareForChecking("SELECT CHARACTER_LENGTH(signature) res FROM action_required.homework_content",
                RunnerType.DEFAULT);
        } catch (RuntimeException ex) {
            Assert.assertTrue(ex.getMessage().contains("Unsupported function"));
        }
    }

    @Test
    public void testDateFunction() {
        prepareForChecking("SELECT YEAR(date_time) FROM action_required.homework_content", RunnerType.DEFAULT)
            .checkExtra("SELECT YEAR(date_time) expr_idx_0 FROM action_required.homework_content");
        prepareForChecking("SELECT MONTH(date_time) FROM action_required.homework_content", RunnerType.DEFAULT)
            .checkExtra("SELECT MONTH(date_time) expr_idx_0 FROM action_required.homework_content");
        prepareForChecking("SELECT DAYOFYEAR(date_time) FROM action_required.homework_content", RunnerType.DEFAULT)
            .checkExtra("SELECT DAYOFYEAR(date_time) expr_idx_0 FROM action_required.homework_content");
        prepareForChecking("SELECT DAYOFWEEK(date_time) FROM action_required.homework_content", RunnerType.DEFAULT)
            .checkExtra("SELECT DAYOFWEEK(date_time) expr_idx_0 FROM action_required.homework_content");
    }

    @Test
    public void testElasticLike() {

    }

    @Test
    public void testInsertInto() {
        prepareForChecking("INSERT INTO `HELLO` IN HDFS SELECT * FROM homework_content");
    }

    @Test
    public void testTrimFunction() {
        prepareForChecking("SELECT TRIM(BOTH ' ' FROM 'hello') FROM student_profile.student", RunnerType.DEFAULT)
            .checkExtra("{\"_source\":[\"city\",\"province\",\"digest\",\"type\",\"stu_id\"]}")
            .checkTrans("SELECT TRIM('hello') AS expr_idx_0 FROM student_profile_student_0");
    }

    //TODO add collection type
    //agg function
    @Test
    public void testElasticUnsupportedFunctions() {
        prepareForChecking("SELECT LENGTH('ddd'), TRIM('bbb'), LOWER(type) "
            + "FROM student_profile.student group by type order by "
            + "type limit 3", RunnerType.DEFAULT)
            .checkExtra("{\"_source\":[\"type\"]}")
            .checkTrans("SELECT LENGTH('ddd') AS expr_idx_0,"
                + " TRIM('bbb') AS expr_idx_1,"
                + " LOWER(type) AS expr_idx_2, type"
                + " FROM student_profile_student_0 GROUP BY type ORDER BY type LIMIT 3");
    }

    @Test
    public void testFunctions() {
        prepareForChecking("SELECT CONCAT(CONCAT('a', 'b', 'c'), 'd')")
            .checkExtra("SELECT CONCAT(CONCAT('a', 'b', 'c'), 'd')");
        prepareForChecking("SELECT SUBSTR(SUBSTR('ACB', 1), 0, 1)")
            .checkExtra("SELECT SUBSTR(SUBSTR('ACB', 1), 0, 1)");
        prepareForChecking("SELECT CONCAT(URLENCODE('ABC'), URLDECODE('%AB'))")
            .checkExtra("SELECT CONCAT(URLENCODE('ABC'), URLDECODE('%AB'))");
        prepareForChecking("SELECT DATEDIFF('2019-01-09', '2019-01-08')")
            .checkExtra("SELECT DATEDIFF('2019-01-09', '2019-01-08')");
        prepareForChecking("SELECT REFLECT('java.net.URLDecoder', 'decode', 'hello', 'UTF-8')")
            .checkExtra("SELECT REFLECT('java.net.URLDecoder', 'decode', 'hello', 'UTF-8')");
        prepareForChecking("SELECT REFLECT('org.apache.commons.lang.math.NumberUtils','isNumber','123')")
            .checkExtra("SELECT REFLECT('org.apache.commons.lang.math.NumberUtils', 'isNumber', '123')");
        prepareForChecking("SELECT REFLECT('java.util.Arrays', 'asList', 'a', 'b', 'c', 'd', 'e')")
            .checkExtra("SELECT REFLECT('java.util.Arrays', 'asList', 'a', 'b', 'c', 'd', 'e')");
    }

    @Test
    public void testWithKeyWord() {
        String sql =
            "with A as (SELECT * FROM action_required.homework_content),B as (select * from action_detection_in_class) "
                + "select A.*,B.* from A,B limit 10";
        prepareForChecking(sql).checkExtra("SELECT t.stu_id, t.date_time, t.signature, t.course_type, t.content,"
            + " t0.stu_id stu_id0, t0.date_time date_time0, t0.action_type "
            + "FROM (SELECT stu_id, date_time, signature, course_type, content"
            + " FROM action_required.homework_content) t, "
            + "(SELECT stu_id, date_time, action_type FROM action_required.action_detection_in_class) t0 LIMIT 10");
    }

    @Test
    public void testSubStringIndexFunction() {
        prepareForChecking( "select substring_index('12-d-d-sd-dff','-',3)");
    }

    @Test
    public void testUnixTimeStampFunction() {
        prepareForChecking("select UNIX_TIMESTAMP()");
        prepareForChecking("select UNIX_TIMESTAMP('2019-11-29')");
        prepareForChecking("select UNIX_TIMESTAMP('2019')");
        prepareForChecking("select UNIX_TIMESTAMP('2019-11-29 11:11')");
        prepareForChecking("select UNIX_TIMESTAMP('2019-11-29 11:11:29')");
    }

    @Test
    public void testFromUnixTimeFunction() {
        prepareForChecking("select FROM_UNIXTIME(1574997071)");
        prepareForChecking("select FROM_UNIXTIME(1)");
        prepareForChecking("select FROM_UNIXTIME(15749970711)");
        prepareForChecking("select FROM_UNIXTIME(1574997071,'yyyy-MM-dd')");
        prepareForChecking("select FROM_UNIXTIME(1574997071,'yyyyMMdd')");
        prepareForChecking("select FROM_UNIXTIME(15749970712,'yyyy-MM-dd')");
    }

    @Test
    public void testLowerAndUpperFunction() {
        prepareForChecking("SELECT signature FROM action_required.homework_content limit 10",
            RunnerType.DEFAULT);
        prepareForChecking("SELECT SIGNATURE FROM action_required.homework_content limit 10",
            RunnerType.DEFAULT);
    }

    @Test
    public void testWhereInCountFunction() {
        prepareForChecking("SELECT signature FROM action_required.homework_content "
                + "WHERE signature IN ('89436','30868','65085','22977','83927', '58429','40697','80614',"
                + "'10502','32771','32772','32773','3274','32743','327733','327724','3277235','327234','327745',"
                + "'3277345','327345','3276','327766','32756') limit 100",
            RunnerType.DEFAULT);
    }

    @Test
    public void testQuote() {
        prepareForChecking("select cast('1998-04-08' as date)",
            RunnerType.DEFAULT);
    }

    @Test
    public void testDateSubFunction() {
        prepareForChecking("select date_sub('2019-12-29',10)");
    }

    @Test
    public void testToDate() {
        prepareForChecking("select to_date('2019-12-02 10:10:10 ')");
    }

    @Test
    public void testCurrentDate() {
        prepareForChecking("select REGEXP_REPLACE(TO_DATE(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 1)), '-', '')");
    }

    @Test
    public void testDay() {
        prepareForChecking("select day from (select 1 as day) a");
    }

    @Test
    public void testCurrenDate() {
        prepareForChecking("select current_date()");
    }

    @Test
    public void testNvl() {
        prepareForChecking("select NVL(type,'xxx') from edu_manage.department");
    }

    private SqlHolder prepareForChecking(String sql) {
        return new SqlHolder(producer.createQueryProcedure(sql));
    }

    private SqlHolder prepareForChecking(String sql, RunnerType runner) {
        QueryProcedureProducer producer = new QueryProcedureProducer(
            "inline: " + MetadataPostman.getCalciteModelSchema(tableNames),
            SqlRunner.builder().setTransformRunner(runner));
        return new SqlHolder(producer.createQueryProcedure(sql));
    }

    private SqlHolder mixed(String sql) {
        QueryProcedure procedure = producer.createQueryProcedure(sql);
        return new SqlHolder(procedure);
    }

    private class SqlHolder {

        QueryProcedure procedure;

        SqlHolder(QueryProcedure procedure) {
            this.procedure = procedure;
        }

        SqlHolder checkExtra(String... expected) {
            List<ExtractProcedure> extractors = new ArrayList<>();
            QueryProcedure current = procedure;
            //change a elegant mode to implement
            do {
                if (current instanceof ExtractProcedure) {
                    extractors.add((ExtractProcedure) current);
                }
                current = current.next();
            }
            while (current != null && current.hasNext());

            List<String> sentences = extractors.stream().map(ExtractProcedure::toRecognizedQuery)
                .sorted().collect(Collectors.toList());

            List<String> expectedSentences = Arrays.stream(expected).sorted()
                .collect(Collectors.toList());
            Assert.assertEquals(expectedSentences.size(), sentences.size());
            for (int i = 0; i < sentences.size(); i++) {
                Assert.assertEquals(expectedSentences.get(i), sentences.get(i));
            }

            return this;
        }

        SqlHolder checkTrans() {
            QueryProcedure current = procedure;
            while (current.hasNext()) {
                if (current instanceof TransformProcedure) {
                    Assert.assertTrue(false);
                }
                current = current.next();
            }
            return this;
        }

        SqlHolder checkTrans(String expected) {
            TransformProcedure transform = null;
            QueryProcedure current = procedure;

            do {
                if (current instanceof TransformProcedure) {
                    transform = ((TransformProcedure) current);
                    break;
                }
                current = current.next();
            }
            while (current != null && current.hasNext());

            Assert.assertTrue(transform != null);
            Assert.assertEquals(expected, transform.sql());
            return this;
        }

        void checkArchitect(String arch) {
            //String[] nodes = arch.split("\\s*->\\s*");
            List<QueryProcedure> procedures = new ArrayList<>();
            QueryProcedure current = procedure;
            procedures.add(current);

            do {
                procedures.add(current.next());
                current = current.next();
            }
            while (current != null && current.hasNext());

            String procAlias = procedures.stream().map(proc -> {
                if (proc instanceof DirectQueryProcedure) {
                    return "[D]";
                } else if (proc instanceof ExtractProcedure) {
                    return "[E]";
                } else if (proc instanceof TransformProcedure) {
                    return "[T]";
                } else if (proc instanceof LoadProcedure) {
                    return "[L]";
                } else {
                    return "[ERROR]";
                }
            }).reduce((left, right) -> left + "->" + right).orElse("");

            Assert.assertEquals(procAlias, arch);
        }
    }
}
