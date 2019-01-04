package com.qihoo.qsql.plan.proc;

import org.apache.calcite.rel.RelNode;

public class PipelineTransformProcedure extends TransformProcedure {
    public PipelineTransformProcedure(QueryProcedure next, RelNode relNode) {
        super(next, relNode);
    }
}
