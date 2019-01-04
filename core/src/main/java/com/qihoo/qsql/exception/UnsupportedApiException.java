package com.qihoo.qsql.exception;

/**
 * Throw exception when use unsupported api.
 */
public class UnsupportedApiException extends QsqlException {

    public UnsupportedApiException(String msg) {
        super(msg);
    }
}
