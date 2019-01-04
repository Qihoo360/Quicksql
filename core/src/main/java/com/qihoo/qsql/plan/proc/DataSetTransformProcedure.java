package com.qihoo.qsql.plan.proc;

import org.apache.calcite.rel.RelNode;

/**
 * Procedure of dataset transformation.
 */
public class DataSetTransformProcedure extends TransformProcedure {

    public DataSetTransformProcedure(QueryProcedure next, RelNode relNode) {
        super(next, relNode);
    }
}
