package com.qihoo.qsql.plan;

import com.qihoo.qsql.plan.proc.DirectQueryProcedure;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.LoadProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.plan.proc.TransformProcedure;

/**
 * Provide several visit methods to traversing the whole {@link QueryProcedure} chain.
 */
public abstract class ProcedureVisitor {

    public abstract void visit(ExtractProcedure extractProcedure);

    public abstract void visit(TransformProcedure transformProcedure);

    public abstract void visit(LoadProcedure loadProcedure);

    public abstract void visit(QueryProcedure queryProcedure);

    public abstract void visit(DirectQueryProcedure queryProcedure);

    protected void visitNext(QueryProcedure procedure) {
        if (procedure.hasNext()) {
            procedure.next().accept(this);
        }
    }
}
