package com.qihoo.qsql.metadata.collect;

import com.qihoo.qsql.metadata.collect.dto.HiveProp;
import com.qihoo.qsql.metadata.entity.ColumnValue;
import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public class HiveCollector extends MetadataCollector {

    //read from mysql database.
    private HiveProp prop;
    private Connection connection;

    HiveCollector(HiveProp prop, String filterRegexp) throws SQLException, ClassNotFoundException {
        super(filterRegexp);
        this.prop = prop;
        Class.forName(prop.getJdbcDriver());
        connection = DriverManager.getConnection(prop.getJdbcUrl(),
            prop.getJdbcUser(), prop.getJdbcPassword());
    }

    @Override
    protected DatabaseValue convertDatabaseValue() {
        DatabaseValue value = new DatabaseValue();
        value.setDbType("hive");
        value.setDesc("Who am I");
        value.setName(getDatabasePosition());
        return value;
    }

    @Override
    protected List<DatabaseParamValue> convertDatabaseParamValue(Long dbId) {
        DatabaseParamValue value = new DatabaseParamValue(dbId);
        value.setParamKey("cluster").setParamValue("default");
        List<DatabaseParamValue> values = new ArrayList<>();
        values.add(value);
        return values;
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
        // read Columns
        String sql = String.format(""
                + "SELECT COLUMNS_V2.* "
                + "FROM COLUMNS_V2, SDS, TBLS, DBS "
                + "WHERE COLUMNS_V2.CD_ID = SDS.CD_ID "
                + "AND SDS.SD_ID = TBLS.SD_ID "
                + "AND TBLS.DB_ID = DBS.DB_ID "
                + "AND TBLS.TBL_NAME='%s' "
                + "AND DBS.NAME = '%s' ",
            tableName, dbName);

        columns.addAll(readColumnAndPartitions(tbId, sql));
        // read Paritions
        String sql2 = String.format(""
                + "SELECT PARTITION_KEYS.* "
                + "FROM PARTITION_KEYS, TBLS, DBS "
                + "WHERE PARTITION_KEYS.TBL_ID = TBLS.TBL_ID "
                + "AND TBLS.DB_ID = DBS.DB_ID "
                + "AND TBLS.TBL_NAME='%s' "
                + "AND DBS.NAME = '%s' ",
            tableName, dbName);
        columns.addAll(readColumnAndPartitions(tbId, sql2));

        return columns;
    }

    @Override
    protected List<String> getTableNameList() {
        if (StringUtils.isEmpty(filterRegexp)) {
            throw new RuntimeException("`Filter regular expression` needed to be set");
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(String.format(
                "SELECT TBL_NAME FROM TBLS INNER JOIN DBS ON TBLS.DB_ID = DBS.DB_ID "
                        + "WHERE TBL_NAME LIKE '%s' AND DBS.NAME ='%s'",
                filterRegexp, prop.getDbName())); ResultSet resultSet = preparedStatement.executeQuery()) {
            List<String> tableNames = new ArrayList<>();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString(1));
            }
            return tableNames;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private String getDatabasePosition() {
        if (prop.getDbName() == null) {
            throw new RuntimeException("Error when extracting dbName from property, "
                + "please check properties");
        }
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT NAME FROM DBS WHERE NAME = \"" + prop.getDbName() + "\"");
             ResultSet resultSet = preparedStatement.executeQuery()) {

            if (! resultSet.next()) {
                throw new RuntimeException("Execute `SELECT NAME FROM DBS WHERE NAME = "
                    + prop.getDbName() + " ` failed!!");
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

    private List<ColumnValue> readColumnAndPartitions(Long tbId, String sql) {
        List<ColumnValue> columns = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                ColumnValue value = new ColumnValue();
                value.setColumnName(resultSet.getString(3));
                value.setTypeName(resultSet.getString(4));
                value.setCdId(tbId);
                value.setIntegerIdx(resultSet.getInt(5));
                value.setComment("Who am I");
                columns.add(value);
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return columns;
    }
}
