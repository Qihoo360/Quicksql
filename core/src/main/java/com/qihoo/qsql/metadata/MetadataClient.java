package com.qihoo.qsql.metadata;

import com.qihoo.qsql.metadata.entity.ColumnValue;
import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import com.qihoo.qsql.metadata.utils.MetaConnectionUtil;
import com.qihoo.qsql.utils.PropertiesReader;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide methods to fetch data from metastore.
 */
//TODO replace with spring+mybatis
public class MetadataClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataClient.class);

    //TODO think about more than one metastore
    private static Properties properties;

    static {
        properties = PropertiesReader.readProperties("metadata.properties");
    }

    private Connection connection;

    /**
     * create interface for accessing metadata.
     *
     * @throws SQLException sql exception
     */
    public MetadataClient() throws SQLException {
        this.connection = createConnection();
    }

    /**
     * select by dbId.
     *
     * @param dbId db identifier
     * @return database value
     */
    public DatabaseValue getBasicDatabaseInfoById(Long dbId) {
        DatabaseValue databaseValue = null;
        String sql = String.format("select DB_ID, NAME, DB_TYPE from DBS where DB_ID ='%d'", dbId);
        LOGGER.debug("getBasicDatabaseInfoById sql is {}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet != null && resultSet.next()) {
                    databaseValue = new DatabaseValue();
                    databaseValue.setDbId(resultSet.getLong("DB_ID"));
                    databaseValue.setName(resultSet.getString("NAME"));
                    databaseValue.setDbType(resultSet.getString("DB_TYPE"));
                }
            }
            return databaseValue;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * select by dbName.
     *
     * @param databaseName database name
     * @return database value
     */
    public DatabaseValue getBasicDatabaseInfo(String databaseName) {
        DatabaseValue databaseValue = null;
        String sql = String.format("select DB_ID, `DESC`, NAME, DB_TYPE from DBS where name ='%s'", databaseName);
        LOGGER.debug("getBasicDatabaseInfo sql is {}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet != null && resultSet.next()) {
                    databaseValue = new DatabaseValue();
                    databaseValue.setDbId(resultSet.getLong("DB_ID"));
                    databaseValue.setName(resultSet.getString("NAME"));
                    databaseValue.setDbType(resultSet.getString("DB_TYPE"));
                    databaseValue.setDesc(resultSet.getString("DESC"));
                }
            }
            return databaseValue;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * insert data value into table.
     *
     * @param value data value
     */
    public Long insertBasicDatabaseInfo(DatabaseValue value) {
        String sql = String.format("INSERT INTO DBS(NAME, DB_TYPE, `DESC`) VALUES('%s', '%s', '%s')",
            value.getName(), value.getDbType(), value.getDesc());
        LOGGER.debug("insertBasicDatabaseInfo sql is {}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return getLastInsertPrimaryKey();
    }

    /**
     * insert schema params into table.
     *
     * @param values database params
     */
    public void insertDatabaseSchema(List<DatabaseParamValue> values) {
        values.removeIf(value -> value.getParamKey().contains("dbName")
            || value.getParamKey().contains("tableName"));
        String waitedForInsert = values.stream().map(value ->
            "(" + value.getDbId() + ", '" + value.getParamKey() + "', '" + value.getParamValue() + "')")
            .reduce((left, right) -> left + ", " + right).orElse("()");
        String sql = String.format("INSERT INTO DATABASE_PARAMS(DB_ID, PARAM_KEY, PARAM_VALUE) VALUES %s",
            waitedForInsert);
        LOGGER.debug("insertDatabaseSchema sql is {}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * d.
     *
     * @param databaseId wait
     * @return wait
     */
    public List<DatabaseParamValue> getDatabaseSchema(Long databaseId) {
        List<DatabaseParamValue> databaseParams = new ArrayList<>();
        String sql = String.format("select PARAM_KEY,PARAM_VALUE from DATABASE_PARAMS where DB_ID='%d'", databaseId);
        LOGGER.debug("getDatabaseSchema sql is {}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet != null) {
                    while (resultSet.next()) {
                        DatabaseParamValue databaseParam = new DatabaseParamValue();
                        databaseParam.setDbId(databaseId);
                        databaseParam.setParamKey(resultSet.getString("PARAM_KEY"));
                        databaseParam.setParamValue(resultSet.getString("PARAM_VALUE"));
                        databaseParams.add(databaseParam);
                    }
                }
            }
            return databaseParams;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * insert table schema into table.
     *
     * @param value data value
     */
    public Long insertTableSchema(TableValue value) {
        String sql = String.format("INSERT INTO TBLS(DB_ID, TBL_NAME) VALUES(%s, '%s')",
            value.getDbId(), value.getTblName());
        LOGGER.debug("insertTableSchema sql is {}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return getLastInsertPrimaryKey();
    }

    /**
     * d.
     *
     * @param tableName wait
     * @return wait
     */
    public List<TableValue> getTableSchema(String tableName) {
        List<TableValue> tbls = new ArrayList<>();
        String sql = String.format("select DB_ID,TBL_ID,TBL_NAME from TBLS where TBL_NAME='%s'", tableName);
        LOGGER.debug("getTableSchema sql is {}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet != null) {
                    while (resultSet.next()) {
                        TableValue tbl = new TableValue();
                        tbl.setDbId(resultSet.getLong("DB_ID"));
                        tbl.setTblId(resultSet.getLong("TBL_ID"));
                        tbl.setTblName(resultSet.getString("TBL_NAME"));
                        tbls.add(tbl);
                    }
                }
            }
            return tbls;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * insert table columns into table.
     *
     * @param columns data value
     */
    public void insertFieldsSchema(List<ColumnValue> columns) {
        String waitedForInsert = columns.stream().map(value ->
            "(" + value.getCdId() + ",'"
                + value.getColumnName() + "','"
                + value.getTypeName() + "', "
                + (columns.indexOf(value) + 1) + ")")
            .reduce((left, right) -> left + ", " + right).orElse("()");
        String sql = String.format(
            "INSERT INTO COLUMNS(CD_ID, COLUMN_NAME,TYPE_NAME,INTEGER_IDX) VALUES %s", waitedForInsert);
        LOGGER.debug("insertFieldsSchema sql is {}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * .
     */
    public void deleteFieldsSchema(Long tbId) {
        String sql = String.format("DELETE FROM COLUMNS WHERE CD_ID = %s", tbId.toString());
        LOGGER.debug("deleteFieldsSchema sql is {}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * d.
     *
     * @param tableId wait
     * @return wait
     */
    public List<ColumnValue> getFieldsSchema(Long tableId) {
        List<ColumnValue> columns = new ArrayList<>();
        String sql = String.format(""
            + "SELECT COLUMN_NAME,TYPE_NAME,INTEGER_IDX FROM COLUMNS WHERE CD_ID='%s' ORDER BY "
            + "INTEGER_IDX", tableId);
        LOGGER.debug("getFieldsSchema sql is {}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet != null) {
                    while (resultSet.next()) {
                        ColumnValue column = new ColumnValue();
                        column.setColumnName(resultSet.getString("COLUMN_NAME"));
                        column.setTypeName(resultSet.getString("TYPE_NAME"));
                        column.setIntegerIdx(resultSet.getInt("INTEGER_IDX"));
                        columns.add(column);
                    }
                }
            }
            return columns;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    //NOT THREAD-SAFE
    private Long getLastInsertPrimaryKey() {
        try (PreparedStatement preparedStatement =
            connection.prepareStatement(getLastInsertSql())) {
            ResultSet resultSet = preparedStatement.executeQuery();
            if (! resultSet.next()) {
                throw new RuntimeException("Execute `SELECT LAST_INSERT_ID()` failed!!");
            }
            return resultSet.getLong(1);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Connection createConnection() throws SQLException {
        if (! MetaConnectionUtil.isEmbeddedDatabase(properties)) {
            return MetaConnectionUtil.getExternalConnection(properties);
        }

        try {
            Class.forName("org.sqlite.JDBC");
            return DriverManager.getConnection("jdbc:sqlite://"
                + new File(properties.getProperty(MetadataParams.META_INTERN_SCHEMA_DIR, "../metastore/schema.db"))
                .getAbsolutePath());
        } catch (ClassNotFoundException | SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setAutoCommit(boolean flag) throws SQLException {
        connection.setAutoCommit(flag);
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    public void rollback() throws SQLException {
        connection.rollback();
    }

    /**
     * close resource.
     */
    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * .
     */
    public String getLastInsertSql() throws SQLException {
        String driver = connection.getMetaData().getDriverName().toLowerCase();
        if (driver.contains("mysql")) {
            return "SELECT LAST_INSERT_ID()";
        } else if (driver.contains("sqlite")) {
            return "SELECT LAST_INSERT_ROWID()";
        } else {
            throw new RuntimeException(
                "Metadata collection for this type of data source is not supported!!");
        }
    }
}
