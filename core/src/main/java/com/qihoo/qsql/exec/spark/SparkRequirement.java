package com.qihoo.qsql.exec.spark;

import com.qihoo.qsql.exec.Requirement;
import org.apache.spark.sql.SparkSession;

/**
 * Build and close Spark environment needed.
 */
public abstract class SparkRequirement implements Requirement {

    protected SparkSession spark;

    protected SparkRequirement(SparkSession sparkSession) {
        this.spark = sparkSession;
    }

    /**
     * close method.
     */
    public void close() {
        if (spark != null) {
            spark.stop();
        }
    }
}
