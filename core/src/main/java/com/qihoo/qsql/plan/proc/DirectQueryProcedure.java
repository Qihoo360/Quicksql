package com.qihoo.qsql.plan.proc;

import com.qihoo.qsql.plan.ProcedureVisitor;
import java.util.List;

/**
 * Represent the calculation which is only in one data source.
 */
public class DirectQueryProcedure extends QueryProcedure {

    public DirectQueryProcedure(QueryProcedure next) {
        super(next);
    }

    @Override
    public int getValue() {
        return 0x11;
    }

    @Override
    public StringBuilder digest(StringBuilder builder, List<String> tabs) {
        String prefix = tabs.stream().reduce((x, y) -> x + y).orElse("");
        tabs.add("\t");
        StringBuilder newBuilder = builder.append(prefix)
            .append("[DirectProcedure]").append("\n");
        if (next() != null) {
            return next().digest(newBuilder, tabs);
        } else {
            return newBuilder;
        }
    }

    @Override
    public void accept(ProcedureVisitor visitor) {
        visitor.visit(this);
    }
}
