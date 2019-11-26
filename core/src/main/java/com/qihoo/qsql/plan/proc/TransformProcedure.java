package com.qihoo.qsql.plan.proc;

import com.qihoo.qsql.plan.ProcedureVisitor;
import com.qihoo.qsql.plan.func.SparkSqlDialect;
import java.util.List;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import com.qihoo.qsql.org.apache.calcite.sql.SqlDialect;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.util.Util;

/**
 * Represent the execution procedure in calculation engine.
 */
public abstract class TransformProcedure extends QueryProcedure {

    private RelNode parent;

    /**
     * Procedure for calculation.
     *
     * @param next next procedure in DAG
     * @param relNode relNode
     */
    public TransformProcedure(QueryProcedure next,
        RelNode relNode) {
        super(next);
        this.parent = relNode;
    }

    /**
     * RelNode to SQL with special Dialect.
     *
     * @return sql
     */
    public String sql() {
        SqlDialect dialect = new SparkSqlDialect(SqlDialect.EMPTY_CONTEXT);
        RelToSqlConverter converter = new RelToSqlConverter(dialect);

        SqlNode sqlNode = converter.visitChild(0, parent).asStatement();
        return Util.toLinux(sqlNode.toSqlString(dialect).getSql()).replaceAll("\n", " ");
    }

    //maybe exists others plan description way
    @Override
    public int getValue() {
        return 0x01;
    }

    @Override
    public String toString() {
        return sql();
    }

    @Override
    public StringBuilder digest(StringBuilder builder, List<String> tabs) {
        String prefix = tabs.stream().reduce((x, y) -> x + y).orElse("");
        tabs.add("\t");
        StringBuilder newBuilder = builder.append(prefix).append("[TransformProcedure]")
            .append("\n").append(prefix)
            .append(" \"logical_node\":").append(parent)
            .append("\n");

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
