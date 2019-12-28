package com.qihoo.qsql.org.apache.calcite.adapter.virtual;

import java.util.Properties;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptCluster;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptTable;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeFactory;
import com.qihoo.qsql.org.apache.calcite.schema.TranslatableTable;
import com.qihoo.qsql.org.apache.calcite.schema.impl.AbstractTable;

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
