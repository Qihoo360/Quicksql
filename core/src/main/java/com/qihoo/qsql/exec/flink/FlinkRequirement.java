package com.qihoo.qsql.exec.flink;

import com.qihoo.qsql.exec.Requirement;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Build and close Flink environment needed.
 */
public abstract class FlinkRequirement implements Requirement {

    protected ExecutionEnvironment env;
    protected BatchTableEnvironment tableEnv;

    protected FlinkRequirement(ExecutionEnvironment environment) {
        this.env = environment;
        this.tableEnv = BatchTableEnvironment.create(env);
    }

    /**
     * close method.
     */
    public void close() {
    }
}
