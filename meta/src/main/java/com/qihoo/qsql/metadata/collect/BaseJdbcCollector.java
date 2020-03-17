package com.qihoo.qsql.metadata.collect;

import com.qihoo.qsql.metadata.ColumnValue;
import com.qihoo.qsql.metadata.collect.dto.HiveProp;
import com.qihoo.qsql.metadata.collect.dto.JdbcProp;
import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import java.util.Date;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseJdbcCollector extends MetadataCollector {

    protected JdbcProp prop;

    protected Connection connection;

    BaseJdbcCollector(String filterRegexp) throws SQLException {
        super(filterRegexp);
    }

    @Override
    protected List<DatabaseParamValue> convertDatabaseParamValue(Long dbId) {
        DatabaseParamValue[] paramValues = new DatabaseParamValue[4];
        for (int i = 0; i < paramValues.length; i++) {
            paramValues[i] = new DatabaseParamValue(dbId);
        }
        paramValues[0].setParamKey("jdbcUrl").setParamValue(prop.getJdbcUrl());
        paramValues[1].setParamKey("jdbcDriver").setParamValue(prop.getJdbcDriver());
        paramValues[2].setParamKey("jdbcUser").setParamValue(prop.getJdbcUser());
        paramValues[3].setParamKey("jdbcPassword").setParamValue(prop.getJdbcPassword());
        return Arrays.stream(paramValues).collect(Collectors.toList());
    }


    @Override
    protected TableValue convertTableValue(Long dbId, String tableName) {
        TableValue value = new TableValue();
        value.setTblName(tableName);
        value.setDbId(dbId);
        value.setCreateTime(new Date().toString());
        return value;
    }

    @Override
    protected List<ColumnValue> convertColumnValue(Long tbId, String tableName, String dbName) {
        List<ColumnValue> columns = new ArrayList<>();
        ResultSet resultSet = null;
        try {
            DatabaseMetaData dbMetadata = connection.getMetaData();
            resultSet = dbMetadata.getColumns(null, dbName.toUpperCase(), tableName.toUpperCase(), "%");
            while (resultSet.next()) {
                String name = resultSet.getString("COLUMN_NAME");
                String type = resultSet.getString("TYPE_NAME");
                int columnIdx = resultSet.getInt("ORDINAL_POSITION");

                ColumnValue value = new ColumnValue();
                value.setColumnName(name);
                value.setTypeName(type);
                value.setCdId(tbId);
                value.setIntegerIdx(columnIdx);
                columns.add(value);
            }
            return columns;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        } finally {
            DbUtils.closeQuietly(resultSet);
        }
    }

    @Override
    protected List<String> getTableNameList() {
        if (StringUtils.isEmpty(filterRegexp)) {
            throw new RuntimeException("`Filter regular expression` needed to be set");
        }

        ResultSet resultSet = null;
        List<String> tableNames = new ArrayList<>();
        try {
            DatabaseMetaData dbMetadata = connection.getMetaData();
            resultSet = dbMetadata.getTables(null, ((HiveProp) prop).getDbName().toUpperCase(), filterRegexp.toUpperCase(),
                    new String[] { "TABLE" });
            while (resultSet.next()) {
                String name = resultSet.getString("TABLE_NAME");
                tableNames.add(name);
            }

            return tableNames;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        } finally {
            DbUtils.closeQuietly(resultSet);
        }
    }
}
