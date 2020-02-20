package com.qihoo.qsql.org.apache.calcite.adapter.virtual;

import com.qihoo.qsql.org.apache.calcite.schema.Path;
import com.qihoo.qsql.org.apache.calcite.schema.Schema;
import com.qihoo.qsql.org.apache.calcite.util.Pair;

import java.util.AbstractList;
import java.util.List;

/**
 * created By QSql team.
 */
public class VirtualPathImpl extends AbstractList<Pair<String, Schema>> implements Path {
    @Override
    public Pair<String, Schema> get(int index) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Path parent() {
        return null;
    }

    @Override
    public List<String> names() {
        return null;
    }

    @Override
    public List<Schema> schemas() {
        return null;
    }
}
