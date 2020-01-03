package com.qihoo.qsql;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.env.RuntimeEnv;
import java.io.IOException;

public class CsvJoinWithEsExample {

    /**
     * If you want to execute in the IDE, adjust the scope of the spark package in the parent pom to compile.
     * @param args nothing
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new RuntimeException("Need to specify the query engine!");
        }
        RuntimeEnv.init();
        String sql = "SELECT * FROM depts "
            + "INNER JOIN (SELECT * FROM student " +
            "WHERE city in ('FRAMINGHAM', 'BROCKTON', 'CONCORD')) FILTERED " +
            "ON depts.name = FILTERED.type ";
        System.out.println("Iput: " + sql);
        SqlRunner.Builder.RunnerType runnerType = RunnerType.value(args[0]);
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(RuntimeEnv.metadata)
            .setAppName("spark-mixed-app")
            .setAcceptedResultsNum(100)
            .ok();
        runner.sql(sql).show();
        System.exit(0);
    }
}
