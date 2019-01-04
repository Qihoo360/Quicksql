package org.apache.calcite.adapter.mysql;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Properties;

public class MySQLTable extends AbstractTable implements TranslatableTable {
    public final String jdbcDriver;
    public final String jdbcUrl;
    public final String jdbcUser;
    public final String jdbcPassword;
    public final String tableName;
    public final String modelUri;
    public final String dbName;

    public Properties properties;

    public Properties getProperties() {
        return properties;
    }

    MySQLTable(String tableName, String dbName,
               String driver, String url, String user,
               String password, String modelUri) {
        this.modelUri = modelUri;
        this.jdbcDriver = driver;
        this.jdbcUrl = url;
        this.jdbcUser = user;
        this.jdbcPassword = password;
        this.tableName = tableName;
        this.dbName = dbName;

        this.properties = new Properties();
        properties.put("jdbcDriver", driver);
        properties.put("jdbcUrl", url);
        properties.put("jdbcUser", user);
        properties.put("jdbcPassword", password);
        properties.put("tableName", tableName);
        properties.put("dbName", dbName);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new MySQLTableScan(cluster, cluster.traitSet(), relOptTable);
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
