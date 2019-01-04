package com.qihoo.qsql;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.env.RuntimeEnv;
import java.io.IOException;

public class CsvJoinWithEsExample {

    public static void main(String[] args) throws IOException {
        RuntimeEnv.init();
        String sql = "SELECT * FROM DEPTS "
            + "INNER JOIN (SELECT * FROM STUDENT " +
            "WHERE city in ('FRAMINGHAM', 'BROCKTON', 'CONCORD')) FILTERED " +
            "ON DEPTS.name = FILTERED.type ";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.SPARK;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(RuntimeEnv.metadata)
            .setAppName("spark-mixed-app")
            .setAcceptedResultsNum(100)
            .ok();
        runner.sql(sql).show().run();
        System.exit(-1);
    }
}
