package com.qihoo.qsql;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.env.RuntimeEnv;
import java.io.IOException;

public class SimpleQueryByFlinkExample {

    /**
     * If you want to execute in the IDE, adjust the scope of the spark package in the parent pom to compile.
     * 如果希望在IDE中执行spark和flink的样例代码，请调整父pom中的spark、flink的scope值为compile。
     * @param args nothing
     */
    public static void main(String[] args) throws IOException {
        RuntimeEnv.init();
        String sql = "select 1";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.value(args.length < 1 ? "flink" : args[0]);
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
            .setSchemaPath(RuntimeEnv.metadata)
            .setAppName("test_csv_app")
            .setAcceptedResultsNum(100)
            .ok();
        runner.sql(sql).show();
        RuntimeEnv.close();
        System.exit(0);
    }
}
