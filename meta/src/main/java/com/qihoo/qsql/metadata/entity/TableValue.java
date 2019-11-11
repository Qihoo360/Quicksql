package com.qihoo.qsql.metadata.entity;

import java.util.Objects;

/**
 * Table related information.
 * <p>
 * tblId, table id
 * dbId, database id
 * createTime, create time for this record
 * tbName, table name
 * </p>
 */
public class TableValue {

    private Long tblId;
    private Long dbId;
    private String createTime;
    private String tblName;

    public TableValue() {}

    public TableValue(Long dbId, String tblName) {
        this.dbId = dbId;
        this.tblName = tblName;
    }

    public Long getTblId() {
        return tblId;
    }

    public void setTblId(Long tblId) {
        this.tblId = tblId;
    }

    public Long getDbId() {
        return dbId;
    }

    public void setDbId(Long dbId) {
        this.dbId = dbId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getTblName() {
        return tblName;
    }

    public void setTblName(String tblName) {
        this.tblName = tblName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TableValue that = (TableValue) obj;
        return Objects.equals(tblId, that.tblId)
            && Objects.equals(dbId, that.dbId)
            && Objects.equals(createTime, that.createTime)
            && Objects.equals(tblName, that.tblName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(tblId, dbId, createTime, tblName);
    }
}
