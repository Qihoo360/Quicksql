package com.qihoo.qsql.metadata.collect;

import com.qihoo.qsql.metadata.ColumnValue;
import com.qihoo.qsql.metadata.collect.dto.JdbcProp;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;


public class JdbcCollector extends BaseJdbcCollector  {
    private Map<String,String> params;
    private String dbType;

    public JdbcCollector(JdbcProp prop, String filter,Map<String,String> params,String dbType) throws
        SQLException,
        ClassNotFoundException {
        super(filter);
        this.prop = prop;
        Class.forName(prop.getJdbcDriver());
        connection = DriverManager.getConnection(prop.getJdbcUrl(),
            prop.getJdbcUser(), prop.getJdbcPassword());
        this.params = params;
        this.dbType = dbType;
    }

    @Override
    protected DatabaseValue convertDatabaseValue() {
        DatabaseValue value = new DatabaseValue();
        value.setDbType(dbType);
        value.setDesc("Who am I");
        value.setName(getDatabasePosition(params.get("showDatabaseSql")));
        return value;
    }

    @Override
    protected List<ColumnValue> convertColumnValue(Long tbId, String tableName, String dbName) {
        if (StringUtils.isNotBlank(dbName) && dbName.contains(".")) {
            String[] split = dbName.split("\\.");
            if (split.length != 2) {
                throw new RuntimeException("convertColumnValue tableName error");
            }
            dbName = split[1];
        }
        String sql = String.format(params.get("columnValueSql"), tableName, dbName);
        List<ColumnValue> columns = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                ColumnValue value = new ColumnValue();
                value.setColumnName(resultSet.getString(1));
                value.setTypeName(resultSet.getString(2));
                value.setCdId(tbId);
                value.setIntegerIdx(resultSet.getInt(3));
                columns.add(value);
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return columns;
    }

    @Override
    protected List<String> getTableNameList() {
        if (StringUtils.isEmpty(filterRegexp)) {
            throw new RuntimeException("`Filter regular expression` needed to be set");
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(
            String.format(params.get("showTableSql"), filterRegexp))) {
            ResultSet resultSet = preparedStatement.executeQuery();
            List<String> tableNames = new ArrayList<>();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString(1));
            }
            return tableNames;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private String getDatabasePosition(String showDatabaseSql) {
        try (PreparedStatement preparedStatement =
            connection.prepareStatement(showDatabaseSql)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            if (! resultSet.next()) {
                throw new RuntimeException("Execute `SELECT DATABASE()` failed!!");
            }
            String database = resultSet.getString(1);
            if (Objects.isNull(database)) {
                throw new RuntimeException("Please add db_name in `jdbcUrl`");
            }
            return database;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}
