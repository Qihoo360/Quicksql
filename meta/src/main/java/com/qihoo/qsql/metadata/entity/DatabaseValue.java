package com.qihoo.qsql.metadata.entity;

import java.util.Objects;

/**
 * Database related information.
 * <p>
 * dbId, the database id
 * desc, descption information for the database
 * name, name of database
 * dbType, database type, such as mysql, elasticsearch, etc.
 * </p>
 */
public class DatabaseValue {

    private Long dbId;
    private String desc;
    private String name;
    private String dbType;

    public Long getDbId() {
        return dbId;
    }

    public void setDbId(Long dbId) {
        this.dbId = dbId;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DatabaseValue that = (DatabaseValue) obj;
        return Objects.equals(dbId, that.dbId)
            && Objects.equals(desc, that.desc)
            && Objects.equals(name, that.name)
            && Objects.equals(dbType, that.dbType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, desc, name, dbType);
    }
}
