package com.qihoo.qsql.metadata.collect;


import com.qihoo.qsql.metadata.collect.dto.ElasticsearchProp;
import com.qihoo.qsql.metadata.entity.ColumnValue;
import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import java.sql.SQLException;
import java.util.List;

public class ElasticsearchCollector extends MetadataCollector {
    private ElasticsearchProp prop;

    public ElasticsearchCollector(ElasticsearchProp prop, String filter) throws SQLException {
        super(filter);
        this.prop = prop;
    }

    @Override
    protected DatabaseValue convertDatabaseValue() {
        DatabaseValue value = new DatabaseValue();
        return null;
    }

    @Override
    protected List<DatabaseParamValue> convertDatabaseParamValue(Long dbId) {
        return null;
    }

    @Override
    protected TableValue convertTableValue(Long dbId, String tableName) {
        return null;
    }

    @Override
    protected List<ColumnValue> convertColumnValue(Long tbId, String tableName, String dbName) {
        return null;
    }

    @Override
    protected List<String> getTableNameList() {
        return null;
    }
}
