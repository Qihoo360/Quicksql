package com.qihoo.qsql.plan;

import java.util.ArrayList;
import java.util.List;

public class QueryTables {
    public List<String> tableNames = new ArrayList<>();
    private boolean isDml = false;

    public boolean isDml() {
        return isDml;
    }

    public void isDmlActually() {
        this.isDml = true;
    }

    void add(String tableName) {
        this.tableNames.add(tableName);
    }
}