package org.apache.calcite.adapter.virtual;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

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
