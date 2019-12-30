package com.qihoo.qsql.org.apache.calcite.adapter.custom;

import com.qihoo.qsql.org.apache.calcite.plan.RelOptCluster;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptTable;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeFactory;
import com.qihoo.qsql.org.apache.calcite.schema.TranslatableTable;
import com.qihoo.qsql.org.apache.calcite.schema.impl.AbstractTable;

import java.util.Properties;

public class JdbcTable extends AbstractTable implements TranslatableTable {
    public final String jdbcDriver;
    public final String jdbcUrl;
    public final String jdbcUser;
    public final String jdbcPassword;
    public final String tableName;
    public final String modelUri;
    public final String dbName;
    public final String dbType;


    public Properties properties;

    public Properties getProperties() {
        return properties;
    }

    JdbcTable(String tableName, String dbName,
               String driver, String url, String user,
               String password, String modelUri, String dbType) {
        this.modelUri = modelUri;
        this.jdbcDriver = driver;
        this.jdbcUrl = url;
        this.jdbcUser = user;
        this.jdbcPassword = password;
        this.tableName = tableName;
        this.dbName = dbName;
        this.dbType = dbType;

        this.properties = new Properties();
        properties.put("jdbcDriver", driver);
        properties.put("jdbcUrl", url);
        properties.put("jdbcUser", user);
        properties.put("jdbcPassword", password);
        properties.put("tableName", tableName);
        properties.put("dbName", dbName);
        properties.put("dbType", dbType);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new JdbcTableScan(cluster, cluster.traitSet(), relOptTable);
    }

    @Override
    public String getBaseName() {
        return dbName;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return super.getRowType(modelUri, dbName, tableName, typeFactory);
    }
}
