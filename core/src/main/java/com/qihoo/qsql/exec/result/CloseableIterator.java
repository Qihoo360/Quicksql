package com.qihoo.qsql.exec.result;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterator interface of reading result that can be close.
 */
public interface CloseableIterator<T> extends Closeable, Iterator<T> {

}
