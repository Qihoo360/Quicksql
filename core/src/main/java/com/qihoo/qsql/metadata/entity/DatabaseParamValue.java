package com.qihoo.qsql.metadata.entity;

import java.util.Objects;

/**
 * Database params related information.
 * <p>
 * dbTd, database id
 * paramKey, param key for database, such as esIndex.
 * paramValue, param value for database, such as 127.0.0.1.
 * </p>
 */
public class DatabaseParamValue {

    private Long dbId;
    private String paramKey;
    private String paramValue;

    public DatabaseParamValue() {}

    public DatabaseParamValue(Long dbId) {
        this.dbId = dbId;
    }
    /**
     * constructor.
     * @param dbId dbId
     * @param paramKey paramKey.
     * @param paramValue paramValue.
     */
    public DatabaseParamValue(Long dbId, String paramKey, String paramValue) {
        this.dbId = dbId;
        this.paramKey = paramKey;
        this.paramValue = paramValue;
    }

    public Long getDbId() {
        return dbId;
    }

    public void setDbId(Long dbId) {
        this.dbId = dbId;
    }

    public String getParamKey() {
        return paramKey;
    }

    public DatabaseParamValue setParamKey(String paramKey) {
        this.paramKey = paramKey;
        return this;
    }

    public String getParamValue() {
        return paramValue;
    }

    public DatabaseParamValue setParamValue(String paramValue) {
        this.paramValue = paramValue;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DatabaseParamValue that = (DatabaseParamValue) obj;
        return Objects.equals(dbId, that.dbId)
            && Objects.equals(paramKey, that.paramKey)
            && Objects.equals(paramValue, that.paramValue);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dbId, paramKey, paramValue);
    }
}
