package org.apache.calcite.adapter.hive;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

public class HiveTableScan extends TableScan {
    protected HiveTableScan(RelOptCluster cluster,
                            RelTraitSet traitSet,
                            RelOptTable table) {
        super(cluster, traitSet, table);
    }
}
