package com.qihoo.qsql.exception;

/**
 * Throw exception when finds that there is no metadata exists.
 */
public class EmptyMetadataException extends QsqlException {

    public EmptyMetadataException(String msg, Exception ex) {
        super(msg, ex);
    }

    public EmptyMetadataException(String msg) {
        super(msg);
    }
}
