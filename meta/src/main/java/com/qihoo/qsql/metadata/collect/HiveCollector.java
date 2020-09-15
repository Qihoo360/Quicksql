package com.qihoo.qsql.metadata.collect;

import com.qihoo.qsql.metadata.collect.dto.HiveProp;
import com.qihoo.qsql.metadata.entity.DatabaseValue;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.DbUtils;

public class HiveCollector extends BaseJdbcCollector {

    public HiveCollector(HiveProp prop, String filter) throws
        SQLException, ClassNotFoundException {
        super(filter);
        this.prop = prop;
        Class.forName(prop.getJdbcDriver());
        connection = DriverManager.getConnection(prop.getJdbcUrl(), prop.getJdbcUser(), prop.getJdbcPassword());
    }

    @Override
    protected DatabaseValue convertDatabaseValue() {
        DatabaseValue value = new DatabaseValue();
        value.setDbType("hive");
        value.setDesc("Who am I");
        value.setName(getDatabasePosition());
        return value;
    }

    private String getDatabasePosition() {
        ResultSet resultSet = null;
        try {
            DatabaseMetaData dbMetadata = connection.getMetaData();
            resultSet = dbMetadata.getSchemas();
            while (resultSet.next()) {
                String schema = resultSet.getString("TABLE_SCHEM");
                if (schema != null && schema.equalsIgnoreCase(((HiveProp) prop).getDbName())) {
                    return schema;
                }
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        } finally {
            DbUtils.closeQuietly(resultSet);
        }
        throw new RuntimeException("Please add db_name in `jdbcUrl`");
    }
}
