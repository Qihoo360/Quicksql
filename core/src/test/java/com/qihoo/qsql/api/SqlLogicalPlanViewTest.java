package com.qihoo.qsql.api;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.plan.QueryProcedureProducer;
import com.qihoo.qsql.plan.proc.EmbeddedElasticsearchPolicy;
import com.qihoo.qsql.plan.proc.QueryProcedureTest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class SqlLogicalPlanViewTest {

    private SqlLogicalPlanView sqlLogicalPlanView = new SqlLogicalPlanView();


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
    public void getLogicalPlanView() {
        String onlyValueSql = "SELECT 'Hello World' AS col1, 1010 AS col2";
        Assert.assertEquals(sqlLogicalPlanView.getLogicalPlanView(onlyValueSql),
            "{\"child\":[null],\"name\":\"LogicalProject\",\"keySet\":[\"fields\"],"
                + "\"commonMap\":{\"fields\":\"[<col1, CHAR(11)>, <col2, INTEGER>]\"}}");

        String withKeyWordsSql = "WITH department AS \n"
            + "    (SELECT times\n"
            + "    FROM edu_manage.department\n"
            + "    GROUP BY  times), student AS \n"
            + "    (SELECT *\n"
            + "    FROM student_profile.student LIMIT 30) \n"
            + "    (SELECT *\n"
            + "    FROM edu_manage.department\n"
            + "    INNER JOIN student\n"
            + "        ON department.times = student.city LIMIT 10)";

        String joinWhereOrderSql = "SELECT dep_id FROM "
            + "edu_manage"
            + ".department inner join (SELECT COUNT(times),times FROM edu_manage.department group by times LIMIT 10 "
            + ") as b on edu_manage.department.times = b.times "
            + "WHERE 1=1 and "
            + "b.times > 2 AND b.times NOT IN(1, 2, 4)  order by b.times";

        String unionSql = "SELECT dep_id FROM edu_manage.department WHERE floor(times) = 1"
            + " union  select dep_id from  edu_manage.department WHERE times = 1";

    }
}