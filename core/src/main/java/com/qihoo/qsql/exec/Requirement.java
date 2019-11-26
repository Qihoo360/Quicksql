package com.qihoo.qsql.exec;

/**
 * Build and close calculation engine context.
 */
public interface Requirement {

    void execute() throws Exception;

    void close();
}
