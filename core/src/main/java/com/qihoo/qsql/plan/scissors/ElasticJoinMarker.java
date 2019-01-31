package com.qihoo.qsql.plan.scissors;


import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

public class ElasticJoinMarker extends LogicalMarker {

    @Override
    public void mark(RelNode child, RelNode parent, int ordinal) {

    }

    @Override
    protected boolean onMatch(RelNode current) {
        if (current == null) {
            return false;
        }

        if (! (current instanceof Join)) {
            return false;
        }

        Join join = (Join) current;
        // if (join.getLeft() instanceof )
        return true;
    }

    private class ElasticLogicalJoin extends Join implements Rollback {
        private Join origin;
        private int ordinal;
        private RelNode parent;

        ElasticLogicalJoin(Join input, RelNode parent, int ordinal) {
            super(input.getCluster(), input.getTraitSet(),
                input.getLeft(), input.getRight(), input.getCondition(),
                input.getVariablesSet(), input.getJoinType());
            this.origin = input;
            this.parent = parent;
            this.ordinal = ordinal;
        }

        @Override
        public Join copy(RelTraitSet traitSet, RexNode conditionExpr,
            RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
            return origin.copy(traitSet, conditionExpr, left, right, joinType, semiJoinDone);
        }

        @Override
        public void rollback() {
            parent.replaceInput(ordinal, origin);
        }
    }
}
