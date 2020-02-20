package com.qihoo.qsql.org.apache.calcite.adapter.hive;

import com.qihoo.qsql.org.apache.calcite.plan.RelOptCluster;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptTable;
import com.qihoo.qsql.org.apache.calcite.plan.RelTraitSet;
import com.qihoo.qsql.org.apache.calcite.rel.core.TableScan;

public class HiveTableScan extends TableScan {
    protected HiveTableScan(RelOptCluster cluster,
                            RelTraitSet traitSet,
                            RelOptTable table) {
        super(cluster, traitSet, table);
    }
}
