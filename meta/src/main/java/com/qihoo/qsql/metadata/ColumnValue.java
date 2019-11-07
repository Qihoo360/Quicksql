package com.qihoo.qsql.metadata;

import java.util.Objects;

/**
 * Column related information.
 * <p>
 * cdId, the table id in table metadata
 * comment, the comment for the column
 * columnName, theZ column name
 * typeName, type name for column, such as int, varchar, etc.
 * integerIdx, order of columns, begin from 0.
 * </p>
 */
public class ColumnValue {

    private Long cdId;
    private String comment;
    private String columnName;
    private String typeName;
    private Integer integerIdx;

    public ColumnValue() {}

    /**
     * constructor.
     * @param cdId cdId
     * @param columnName columnName
     * @param typeName typeName
     */
    public ColumnValue(Long cdId, String columnName, String typeName) {
        this.cdId = cdId;
        this.columnName = columnName;
        this.typeName = typeName;
    }

    public Long getCdId() {
        return cdId;
    }

    public void setCdId(Long cdId) {
        this.cdId = cdId;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Integer getIntegerIdx() {
        return integerIdx;
    }

    public void setIntegerIdx(Integer integerIdx) {
        this.integerIdx = integerIdx;
    }

    @Override
    public String toString() {
        return "{\"name\": \"" + columnName + ":" + typeName + "\"}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ColumnValue that = (ColumnValue) obj;
        return Objects.equals(cdId, that.cdId)
            && Objects.equals(columnName, that.columnName)
            && Objects.equals(typeName, that.typeName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(cdId, comment, columnName, typeName, integerIdx);
    }
}
