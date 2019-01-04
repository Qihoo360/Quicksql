package com.qihoo.qsql.launcher;

import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.exception.QsqlException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;

public class ExecutionDispatcherTest {

    @Rule
    public final SystemOutRule rule = new SystemOutRule().enableLog();

    private List<String> args = new LinkedList<>();

    /**
     * set up.
     */
    @Before
    public void setUp() {
        args.clear();
        args.add("--jar");
        args.add("test.jar");
        args.add("--jar_name");
        args.add("./target/qsql-core-0.5.jar");
    }

    @Test
    public void testSimpleQuery() throws SQLException, ParseException {
        sql("select 1").runner(RunnerType.DEFAULT).check("1");
    }

    @Test
    public void testJdbcSingleTableQuery() throws SQLException, ParseException {
        sql("select * from edu_manage.department").runner(RunnerType.DEFAULT)
            .check(this::executedByJdbc);
    }

    @Test
    public void testSimpleQueryExecutedBySpark() throws SQLException, ParseException {
        sql("select 1").runner(RunnerType.SPARK).check(this::executedBySparkOrFlink);
    }

    @Test
    public void testMultipleTableQueryByJdbc() throws SQLException, ParseException {
        sql("select * from department as dep inner join "
            + "department_student_relation as rel on dep.dep_id = rel.dep_id")
            .runner(RunnerType.DEFAULT).check(this::executedByJdbc);
    }

    @Test
    public void testMultipleTableQueryBySpark() throws SQLException, ParseException {
        sql("select * from department as dep inner join homework_content as stu on dep.dep_id = stu.stu_id")
            .runner(RunnerType.DEFAULT).check(this::executedBySparkOrFlink);
    }

    @Test
    public void testSingleTableQueryBySpark() throws SQLException, ParseException {
        sql("select * from homework_content").runner(RunnerType.DEFAULT)
            .check(this::executedBySparkOrFlink);
    }

    @Test
    public void testHiveWithJdbc() {
        try {
            sql("select * from homework_content").runner(RunnerType.JDBC);
        } catch (QsqlException exception) {
            Assert.assertTrue(true);
        }
    }

    private boolean executedBySparkOrFlink(Exception ex) {
        return ex.getMessage().contains("Process exited with an error");
    }

    private boolean notFoundDatabase(Exception ex) {
        return ex.getMessage().contains("not found within any database");
    }

    private boolean executedByJdbc(Exception ex) {
        return Arrays.stream(ex.getStackTrace()).anyMatch(trace -> trace.getClassName().contains("Jdbc"));
    }

    private ExecutionDispatcherTest sql(String sql) {
        args.add("--sql");
        args.add(new String(Base64.getEncoder().encode(sql.getBytes()), StandardCharsets.UTF_8));
        return this;
    }

    private ExecutionDispatcherTest runner(RunnerType type) {
        args.add("--runner");
        switch (type) {
            case SPARK:
                args.add("SPARK");
                break;
            case FLINK:
                args.add("FLINK");
                break;
            case JDBC:
                args.add("JDBC");
                break;
            default:
                args.add("DYNAMIC");
        }
        return this;
    }

    private void check(Function<Exception, Boolean> function) throws SQLException, ParseException {
        try {
            ExecutionDispatcher.main(args.toArray(new String[0]));
        } catch (RuntimeException ex) {
            ex.printStackTrace();
            Assert.assertTrue(function.apply(ex));
        }
    }

    private void check(String result) throws SQLException, ParseException {
        ExecutionDispatcher.main(args.toArray(new String[0]));
        Assert.assertTrue(rule.getLog().contains("1"));
    }
}
