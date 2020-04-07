package com.qihoo.qsql.plan.proc;

import com.qihoo.qsql.org.apache.calcite.rel.TreeNode;
import com.qihoo.qsql.plan.ProcedureVisitor;
import com.qihoo.qsql.plan.Traversable;

import java.util.List;

/**
 * QueryProcedure, the basic description in QSql for SQL. <p> In QSql, a SQL line is divided into sub-SQLs for different
 * function in execution. Each QueryProcedure can be regarded as a complete query. The relation chain between
 * QueryProcedure decide the order of execution in calculation engine. Considering that SQL is a tree instead of a graph
 * which can be executed into part and in order, like a chain, that is the main reason for trying to abstract SQL into a
 * QueryProcedure. </p> <p> For example, a SQL query "SELECT A.a1, B.b1 FROM a as A, b as B WHERE A.a1 = B.b1" will be
 * divide into 4 procedure which represent 3 sub-SQLs: </p> <p>(1) (SELECT a1 FROM a) AS A </p> <p>(2) (SELECT b1 FROM
 * b) AS B </p> <p>(3) SELECT A.a1, B.b1 FROM (A JOIN B ON (A.a1 = B.b1)) </p> <p> And the QueryProcedure chain will be
 * </p> <p> {@link ExtractProcedure} - {@link ExtractProcedure} - {@link TransformProcedure} - {@link LoadProcedure}
 * </p> <p> The last procedure {@link LoadProcedure} decides the method to show result.</p>
 */
public abstract class QueryProcedure implements Traversable {

    private QueryProcedure next;

    public QueryProcedure(QueryProcedure next) {
        this.next = next;
    }

    public boolean hasNext() {
        return next != null;
    }

    public void resetNext(QueryProcedure procedure) {
        this.next = procedure;
    }

    public QueryProcedure next() {
        return next;
    }

    private TreeNode treeNode;

    public TreeNode getTreeNode() {
        return treeNode;
    }

    public void setTreeNode(TreeNode treeNode) {
        this.treeNode = treeNode;
    }

    @Override
    public void accept(ProcedureVisitor visitor) {
        visitor.visit(this);
    }

    public abstract int getValue();

    @Override
    public String toString() {
        return "No meaning";
    }

    public abstract StringBuilder digest(StringBuilder builder, List<String> tabs);
}
