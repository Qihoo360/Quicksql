package com.qihoo.qsql.exec.result;

import org.apache.commons.io.LineIterator;

import java.io.Reader;

/**
 * Iterator of reading data from HDFS.
 */
public class FileLineIterator extends LineIterator implements AutoCloseable {

    public FileLineIterator(Reader reader) throws IllegalArgumentException {
        super(reader);
    }

}
