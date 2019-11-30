package com.qihoo.qsql.exec.result;

import java.sql.ResultSet;

/**
 * Result of pipeline.
 */
public interface PipelineResult {

    ResultSet getData();

    void run();
}

