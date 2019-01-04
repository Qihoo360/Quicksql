package com.qihoo.qsql.exception;

/**
 * Throw exception when find errors in qsql.
 */
public class QsqlException extends RuntimeException {

    public QsqlException(String msg) {
        super(msg);
    }

    public QsqlException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
