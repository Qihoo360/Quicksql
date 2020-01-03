package com.qihoo.qsql;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.env.RuntimeEnv;
import java.io.IOException;

public class CsvScanExample {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new RuntimeException("Need to specify the query engine!");
        }
        RuntimeEnv.init();
        String sql = "select * from depts";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.value(args[0]);
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(RuntimeEnv.metadata)
            .setAppName("test_csv_app")
            .setAcceptedResultsNum(100)
            .ok();
        runner.sql(sql).show();
        System.exit(0);
    }
}
