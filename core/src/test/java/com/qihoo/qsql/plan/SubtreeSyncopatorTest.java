package com.qihoo.qsql.plan;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.exception.ParseException;
import com.qihoo.qsql.exception.QsqlException;
import com.qihoo.qsql.exec.JdbcPipeline;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.plan.func.SqlRunnerFuncTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import com.qihoo.qsql.org.apache.calcite.model.ModelHandler;
import com.qihoo.qsql.org.apache.calcite.plan.RelTraitDef;
import com.qihoo.qsql.org.apache.calcite.plan.hep.HepPlanner;
import com.qihoo.qsql.org.apache.calcite.plan.hep.HepProgram;
import com.qihoo.qsql.org.apache.calcite.plan.hep.HepProgramBuilder;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.rules.SubQueryRemoveRule;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParseException;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParser;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlConformanceEnum;
import com.qihoo.qsql.org.apache.calcite.sql2rel.SqlToRelConverter;
import com.qihoo.qsql.org.apache.calcite.tools.FrameworkConfig;
import com.qihoo.qsql.org.apache.calcite.tools.Frameworks;
import com.qihoo.qsql.org.apache.calcite.tools.Planner;
import com.qihoo.qsql.org.apache.calcite.tools.RelConversionException;
import com.qihoo.qsql.org.apache.calcite.tools.ValidationException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link SubtreeSyncopator}.
 */
public class SubtreeSyncopatorTest {

    @Test
    public void testSimpleSqlWithoutTable() {
        String sql = "SELECT 1";
        Set<RelNode> result = new Sql(sql).exec();
        String[] expect = {"LogicalProject-LogicalValues"};
        Assert.assertArrayEquals("testSimpleSqlWithoutTable", expect, sortRelNode(result).toArray());
    }

    @Test
    public void testSimpleSqlWithMySql() {
        String sql = "SELECT dep_id FROM edu_manage.department WHERE dep_id = 1";
        Set<RelNode> result = new Sql(sql).exec();
        String[] expect = {"LogicalProject-LogicalFilter-JdbcTableScan"};
        Assert.assertArrayEquals("testSimpleSqlWithMySql", expect, sortRelNode(result).toArray());
    }

    @Test
    public void testSimpleSqlWithMySqlAggregate() {
        String sql = "SELECT times, SUM(dep_id) FROM edu_manage.department"
            + " WHERE times = 1 GROUP BY times";
        Set<RelNode> result = new Sql(sql).exec();
        String[] expect = {"LogicalAggregate-LogicalProject-LogicalFilter-JdbcTableScan"};
        Assert.assertArrayEquals("testSimpleSqlWithMySqlAggregate", expect, sortRelNode(result).toArray());
    }

    @Test
    public void testMixSqlWithJoin() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM edu_manage.department AS a, action_required.homework_content AS b"
            + " WHERE a.dep_id = b.stu_id";
        Set<RelNode> result = new Sql(sql).exec();
        String[] expect = {"LogicalProject-JdbcTableScan", "LogicalProject-HiveTableScan"};
        Assert.assertArrayEquals("test", expect, sortRelNode(result).toArray());
    }

    //for complex join sql, logicalPlan will create LogicalProject first
    @Test
    public void testMixSqlWithJoinAndFilter() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM (SELECT dep_id FROM edu_manage.department WHERE dep_id = 1) AS a"
            + " JOIN (SELECT stu_id FROM action_required.homework_content) AS b"
            + " ON(a.dep_id = b.stu_id)";
        Set<RelNode> result = new Sql(sql).exec();
        String[] expect = {"LogicalProject-LogicalProject-LogicalFilter-JdbcTableScan",
            "LogicalProject-LogicalProject-HiveTableScan"};
        Assert.assertArrayEquals("testMixSqlWithJoinAndFilter", expect, sortRelNode(result).toArray());
    }

    @Test
    public void testMixSqlWithUnion() {
        String sql = " (SELECT dep_id FROM edu_manage.department WHERE dep_id = 1)"
            + " UNION (SELECT stu_id FROM action_required.homework_content) ";
        Set<RelNode> result = new Sql(sql).exec();
        String[] expect = {"LogicalProject-LogicalProject-LogicalFilter-JdbcTableScan",
            "LogicalProject-LogicalProject-HiveTableScan"};
        Assert.assertArrayEquals("testMixSqlWithUnion", expect, sortRelNode(result).toArray());
    }

    //subQuery need optimize by hepPlanner, and then turn to join
    @Test
    public void testMixSqlWithSubQueryInSelect() {
        String sql = "SELECT dep_id, (SELECT COUNT(stu_id) FROM action_required.homework_content)"
            + " FROM edu_manage.department WHERE dep_id = 1";
        Set<RelNode> result = new Sql(sql).execOptimize();
        String[] expect = {"LogicalProject-LogicalFilter-JdbcTableScan",
            "LogicalProject-LogicalAggregate-LogicalProject-HiveTableScan"};
        Assert.assertArrayEquals("testMixSqlWithUnion", expect, sortRelNode(result).toArray());
    }

    @Test
    public void testMixSqlWithSubQueryInWhereExist() {
        String sql = "SELECT dep_id FROM edu_manage.department WHERE EXISTS "
            + " (SELECT stu_id FROM action_required.homework_content )";
        Set<RelNode> result = new Sql(sql).execOptimize();
        String[] expect = {"LogicalProject-JdbcTableScan",
            "LogicalProject-LogicalAggregate-LogicalProject-HiveTableScan"};
        Assert.assertArrayEquals("testMixSqlWithUnion", expect, sortRelNode(result).toArray());
    }


    @Test
    public void testMixSqlWithSubQueryInWhereIn() {
        String sql = "SELECT dep_id FROM edu_manage.department WHERE dep_id IN"
            + " (SELECT stu_id FROM action_required.homework_content)";
        Set<RelNode> result = new Sql(sql).execOptimize();
        String[] expect = {"LogicalProject-JdbcTableScan",
            "LogicalProject-LogicalAggregate-LogicalProject-HiveTableScan"};
        Assert.assertArrayEquals("testMixSqlWithUnion", expect, sortRelNode(result).toArray());
    }

    @Test
    public void testSimpleSqlWithAndWithoutTableName() {
        String sql = "SELECT a.dep_id, b.stu_id"
            + " FROM (SELECT dep_id FROM edu_manage.department WHERE dep_id = 1) AS a"
            + " JOIN (SELECT 1 as stu_id) AS b"
            + " ON(a.dep_id = b.stu_id)";
        Set<RelNode> result = new Sql(sql).exec();
        String[] expect = {
            "LogicalProject-LogicalJoin-LogicalProject-LogicalFilter-JdbcTableScan-LogicalProject-LogicalValues"
        };
        Assert.assertArrayEquals("testSimpleSqlWithAndWithoutTableName",
            expect, sortRelNode(result).toArray());
    }

    // @Test
    // public void testMixSqlWithJoinBetweenDifferentDb() {
    //     String sql = "(SELECT a.dep_id FROM "
    //         + "  (SELECT dep_id FROM edu_manage.department) AS a "
    //         + "  JOIN "
    //         + "  (SELECT id FROM edu_manage.department_student_relation) AS b "
    //         + "  ON(a.dep_id = b.id) "
    //         + " ) "
    //         + " UNION "
    //         + " (SELECT dep_id FROM edu_manage.department)";
    //     Set<RelNode> result = new Sql(sql).exec();
    //     String[] expect = {
    //         "LogicalProject-LogicalProject-LogicalJoin-LogicalProject-JdbcTableScan-LogicalProject-JdbcTableScan",
    //         "LogicalProject-LogicalProject-JdbcTableScan"};
    //     Assert.assertArrayEquals("testMixSqlWithJoinBetweenDifferentDb",
    //         expect, sortRelNode(result).toArray());
    // }

    // @Test
    // public void testMixSqlWithJoinBetweenDifferentDb2() {
    //     String sql = " (SELECT a.dep_id FROM "
    //         + "  (SELECT dep_id FROM edu_manage.department) AS a "
    //         + "  JOIN "
    //         + "  (SELECT dep_id FROM edu_manage.department) AS b "
    //         + "  ON(a.dep_id = b.dep_id) "
    //         + " ) "
    //         + " UNION "
    //         + " (SELECT id FROM edu_manage.department_student_relation)";
    //     Set<RelNode> result = new Sql(sql).exec();
    //     String[] expect = {
    //         "LogicalProject-LogicalProject-JdbcTableScan",
    //         "LogicalProject-LogicalProject-JdbcTableScan",
    //         "LogicalProject-LogicalProject-JdbcTableScan"};
    //     Assert.assertArrayEquals("testMixSqlWithJoinBetweenDifferentDb2",
    //         expect, sortRelNode(result).toArray());
    // }

    private List<String> sortRelNode(Set<RelNode> result) {
        List<String> printToString = new ArrayList<>();

        for (RelNode relNode : result) {
            StringBuilder stringBuilder = new StringBuilder();
            printAllRelNode(relNode, stringBuilder);
            printToString.add(stringBuilder.deleteCharAt(stringBuilder.lastIndexOf("-")).toString());
        }
        return printToString;
    }

    private StringBuilder printAllRelNode(RelNode relNode, StringBuilder result) {
        if (relNode.getDigest().contains(".")) {
            //for subQuery
            result.append(relNode.getDigest().split("\\.")[0]).append("-");
        } else {
            result.append(relNode.getDigest().split("#")[0]).append("-");
        }
        List<RelNode> childs = relNode.getInputs();
        for (RelNode child : childs) {
            printAllRelNode(child, result);
        }
        return result;
    }

    static class Sql {

        String sql = null;
        private FrameworkConfig config = null;

        public Sql(String sql) {
            this.sql = sql;
            init();
        }

        SqlRunnerFuncTable funcTable = SqlRunnerFuncTable.getInstance(RunnerType.DEFAULT);
        Builder builder = SqlRunner.builder().setTransformRunner(RunnerType.DEFAULT);

        private static List<String> parseTableName(String sql) {
            TableNameCollector collector = new TableNameCollector();
            try {
                return new ArrayList<>(collector.parseTableName(sql).tableNames);
            } catch (SqlParseException ex) {
                throw new RuntimeException(ex.getMessage());
            }
        }

        Set<RelNode> exec() {
            if (this.sql == null) {
                throw new QsqlException("Please init SQL first");
            }

            RelNode origin = buildLogicalPlan(this.sql);
            SubtreeSyncopator subtreeSyncopator = new SubtreeSyncopator(origin, funcTable, builder);
            return subtreeSyncopator.rootNodeSchemas.keySet();
        }

        //for subQuery
        Set<RelNode> execOptimize() {
            if (this.sql == null) {
                throw new QsqlException("Please init SQL first");
            }

            RelNode origin = buildLogicalPlan(this.sql);
            RelNode optimize = optimizeLogicalPlan(origin);
            SubtreeSyncopator subtreeSyncopator = new SubtreeSyncopator(optimize, funcTable, builder);
            return subtreeSyncopator.rootNodeSchemas.keySet();
        }

        private void init() {
            try {
                String path = getSchemaPath(parseTableName(sql));
                if (path.equals("inline: ")) {
                    path = JdbcPipeline.CSV_DEFAULT_SCHEMA;
                }
                initPlannerConfig(path);
            } catch (IOException ex) {
                throw new QsqlException("test error");
            }
        }

        private String getSchemaPath(List<String> tableNames) {
            return "inline: " + MetadataPostman.getCalciteModelSchema(tableNames);
        }

        private void initPlannerConfig(String jsonPath) throws IOException {
            final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

            new ModelHandler(rootSchema, jsonPath);
            final SqlToRelConverter.Config convertConfig = SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(false)
                .withConvertTableAccess(false)
                .withExpand(false)
                .build();

            final SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setConformance(SqlConformanceEnum.MYSQL_5)
                .setQuoting(Quoting.BACK_TICK)
                .setUnquotedCasing(Casing.UNCHANGED)
                // .setUnquotedCasing(Casing.UNCHANGED)
                .build();

            this.config = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(rootSchema)
                .traitDefs((List<RelTraitDef>) null)
                .sqlToRelConverterConfig(convertConfig)
                .build();
        }

        private RelNode buildLogicalPlan(String sql) {
            Planner planner = Frameworks.getPlanner(config);

            try {
                SqlNode parsed = planner.parse(sql);
                SqlNode validated = planner.validate(parsed);
                return planner.rel(validated).rel;
            } catch (SqlParseException ex) {
                throw new ParseException("Error When Parsing Origin SQL: " + ex.getMessage(), ex);
            } catch (ValidationException | RelConversionException ev) {
                throw new ParseException("Error When Validating: " + ev.getMessage(), ev);
            } catch (Throwable ex) {
                throw new ParseException("Unknown Parse Exception, Concrete Message is: " + ex.getMessage());
            }
        }

        private RelNode optimizeLogicalPlan(RelNode root) {
            final HepProgram program = new HepProgramBuilder()
                .addRuleInstance(SubQueryRemoveRule.PROJECT)
                .addRuleInstance(SubQueryRemoveRule.FILTER)
                .addRuleInstance(SubQueryRemoveRule.JOIN)
                .build();

            HepPlanner prePlanner = new HepPlanner(program);
            prePlanner.setRoot(root);

            return prePlanner.findBestExp();
        }

    }

}
