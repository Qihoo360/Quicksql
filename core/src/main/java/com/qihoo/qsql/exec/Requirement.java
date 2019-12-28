package com.qihoo.qsql.exec;


/**
 * Build and close calculation engine context.
 */
public interface Requirement {

    Object execute();

    void close();
}
