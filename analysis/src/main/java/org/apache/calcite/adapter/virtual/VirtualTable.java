package org.apache.calcite.adapter.virtual;

import java.util.Properties;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * created By QSql team.
 */
public class VirtualTable extends AbstractTable implements TranslatableTable {

    private String db;
    private Properties properties = new Properties();

    public VirtualTable(String db) {
        this.db = db;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new VirtualTableScan(cluster, cluster.traitSet(), relOptTable);
    }

    @Override
    public String getBaseName() {
        return db;
    }
}
