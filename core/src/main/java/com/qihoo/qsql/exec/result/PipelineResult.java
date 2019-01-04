package com.qihoo.qsql.exec.result;

import java.util.Collection;

/**
 * Result of pipeline.
 */
public interface PipelineResult {

    Collection<String> getData();

    void run();
}

