package com.qihoo.qsql.plan.scissors;

import org.apache.calcite.rel.RelNode;

/**
 * .
 */

public abstract class LogicalMarker {

    protected abstract void mark(RelNode child, RelNode parent, int ordinal);

    protected abstract boolean onMatch(RelNode current);

    /**
     * .
     */
    public void transpose(RelNode child, RelNode parent, int ordinal) {
        if (onMatch(child)) {
            mark(child, parent, ordinal);
        }
    }

    protected interface Rollback {
        void rollback();
    }

    protected class Memento {
        private RelNode past;

        public Memento(RelNode past) {
            this.past = past;
        }

        public RelNode getPast() {
            return past;
        }
    }
}
