package com.qihoo.qsql.exception;

/**
 * Throw exception when find errors in parsing params.
 */
public class ParseException extends QsqlException {

    public ParseException(String msg) {
        super(msg);
    }

    public ParseException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
