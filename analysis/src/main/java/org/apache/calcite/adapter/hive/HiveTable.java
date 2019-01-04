
package org.apache.calcite.adapter.hive;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Properties;

/**
 * Table mapped onto a Hive table
 */
public class HiveTable extends AbstractTable implements TranslatableTable {

    public final String dbName;
    public final String cluster;
    public final String tableName;
    public final String modelUri;
    public Properties properties;

    public Properties getProperties() {
        return properties;
    }

    HiveTable(String dbName, String cluster, String tableName, String modelUri) {
        this.dbName = dbName;
        this.cluster = cluster;
        this.tableName = tableName;
        this.modelUri = modelUri;
        this.properties = new Properties();
        this.properties.put("dbName", dbName);
        this.properties.put("cluster", cluster);
        this.properties.put("tableName", tableName);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return super.getRowType(modelUri, dbName, tableName, typeFactory);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new HiveTableScan(cluster, cluster.traitSet(), relOptTable);
    }

    @Override
    public String getBaseName() {
        return dbName;
    }
}
