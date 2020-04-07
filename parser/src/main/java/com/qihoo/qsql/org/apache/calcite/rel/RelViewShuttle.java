package com.qihoo.qsql.org.apache.calcite.rel;

import com.qihoo.qsql.org.apache.calcite.rel.core.TableFunctionScan;
import com.qihoo.qsql.org.apache.calcite.rel.core.TableScan;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalAggregate;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalCorrelate;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalExchange;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalFilter;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalIntersect;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalJoin;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalMatch;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalMinus;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalProject;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalSort;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalUnion;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalValues;

/**
 * Visitor that has methods for the common logical relational expressions convert to node of tree.
 */
public interface RelViewShuttle {

    TreeNode visit(TableScan scan);

    TreeNode visit(TableFunctionScan scan);

    TreeNode visit(LogicalValues values);

    TreeNode visit(LogicalFilter filter);

    TreeNode visit(LogicalProject project);

    TreeNode visit(LogicalJoin join);

    TreeNode visit(LogicalCorrelate correlate);

    TreeNode visit(LogicalUnion union);

    TreeNode visit(LogicalIntersect intersect);

    TreeNode visit(LogicalMinus minus);

    TreeNode visit(LogicalAggregate aggregate);

    TreeNode visit(LogicalMatch match);

    TreeNode visit(LogicalSort sort);

    TreeNode visit(LogicalExchange exchange);

    TreeNode visit(RelNode other);
}
// End RelViewShuttle.java