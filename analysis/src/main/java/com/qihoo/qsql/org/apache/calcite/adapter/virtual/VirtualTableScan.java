package com.qihoo.qsql.org.apache.calcite.adapter.virtual;

import com.qihoo.qsql.org.apache.calcite.plan.RelOptCluster;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptTable;
import com.qihoo.qsql.org.apache.calcite.plan.RelTraitSet;
import com.qihoo.qsql.org.apache.calcite.rel.core.TableScan;

/**
 * created By QSql team.
 */
public class VirtualTableScan extends TableScan {

    public VirtualTableScan(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table) {
        super(cluster, traitSet, table);
    }
}
