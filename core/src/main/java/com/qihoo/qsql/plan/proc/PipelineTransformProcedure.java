package com.qihoo.qsql.plan.proc;

import com.qihoo.qsql.org.apache.calcite.rel.RelNode;

public class PipelineTransformProcedure extends TransformProcedure {
    public PipelineTransformProcedure(QueryProcedure next, RelNode relNode) {
        super(next, relNode);
    }
}
