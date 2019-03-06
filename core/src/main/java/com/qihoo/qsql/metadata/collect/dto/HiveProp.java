package com.qihoo.qsql.metadata.collect.dto;

import javax.validation.constraints.NotNull;

public class HiveProp extends JdbcProp {

    @NotNull
    private String dbName;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
