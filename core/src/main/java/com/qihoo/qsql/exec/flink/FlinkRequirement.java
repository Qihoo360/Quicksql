package com.qihoo.qsql.exec.flink;

import com.qihoo.qsql.exec.Requirement;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Build and close Flink environment needed.
 */
public abstract class FlinkRequirement implements Requirement {

    protected ExecutionEnvironment env;
    protected TableEnvironment tableEnv;

    protected FlinkRequirement(ExecutionEnvironment environment) {
        this.env = environment;
        this.tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    /**
     * close method.
     */
    public void close() {
    }
}
