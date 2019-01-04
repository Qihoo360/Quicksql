package com.qihoo.qsql.exec.result;

import java.util.NoSuchElementException;

/**
 * Iterator of reading data from ResultSet.
 */
public abstract class LookaheadIterator<T> implements CloseableIterator<T> {

    protected T next;

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }

        if (! doesHaveNext()) {
            return false;
        }

        return getNext();
    }

    /**
     * By default we can return true. since our logic does not rely on hasNext() - it pre-fetches the next
     */
    private boolean doesHaveNext() {
        return true;
    }

    protected boolean getNext() {
        next = loadNext();

        return next != null;
    }

    protected abstract T loadNext();

    @Override
    public T next() {
        if (! hasNext()) {
            throw new NoSuchElementException();
        }
        T result = next;

        next = null;

        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
