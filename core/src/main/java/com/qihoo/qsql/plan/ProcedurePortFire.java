package com.qihoo.qsql.plan;

import com.qihoo.qsql.plan.proc.DirectQueryProcedure;
import com.qihoo.qsql.plan.proc.DiskLoadProcedure;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.LoadProcedure;
import com.qihoo.qsql.plan.proc.PreparedExtractProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.plan.proc.TransformProcedure;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Re-generate the QueryProcedure, including adjusting and optimizing the QueryProcedure chain.
 */
public class ProcedurePortFire {

    private List<ExtractProcedure> procedures;

    public ProcedurePortFire(List<ExtractProcedure> procedures) {
        this.procedures = procedures;
    }

    /**
     * If the SQL is a special SQL, then the query result will not put in tempTable and re-Calculate by Spark.
     */
    public QueryProcedure optimize() {
        //do optimize like reducing rel node

        List<QueryProcedure> sortedProcedures = flattenProcedures();

        assert ! sortedProcedures.isEmpty() : "A extract procedure must be here";

        QueryProcedure currHead = sortedProcedures.get(0);
        //just leave one extract procedure
        if (procedures.size() == 1 && ! hasDiskLoad(currHead)) {
            //as a part of optimization, this block should move to package 'opt'
            if (procedures.get(0) instanceof PreparedExtractProcedure.VirtualExtractor) {
                for (QueryProcedure curr = currHead,
                    next = currHead.next(); curr.hasNext(); curr = curr.next()) {
                    if (next instanceof TransformProcedure) {
                        curr.resetNext(next.next());
                    }
                }
            }
            return new DirectQueryProcedure(currHead);
        } else {
            return currHead;
        }
    }

    private List<QueryProcedure> flattenProcedures() {
        List<QueryProcedure> procedures =
            new FlattenProcedureVisitor(this).sort();

        for (int i = 0; i < procedures.size() - 1; i++) {
            if (! (procedures.get(i) instanceof LoadProcedure)) {
                procedures.get(i).resetNext(procedures.get(i + 1));
            }
        }

        return procedures;
    }

    //like sqoop
    private boolean hasDiskLoad(QueryProcedure currHead) {
        if (! currHead.hasNext()) {
            return currHead instanceof DiskLoadProcedure;
        }
        return hasDiskLoad(currHead.next());
    }

    private class FlattenProcedureVisitor extends ProcedureVisitor {

        Set<QueryProcedure> flatProcedures = new HashSet<>();

        FlattenProcedureVisitor(ProcedurePortFire fire) {
            fire.procedures.forEach(proc -> proc.accept(this));
        }

        List<QueryProcedure> sort() {
            return flatProcedures.stream().sorted(
                (left, right) -> left.getValue() > right.getValue() ? - 1 : 1)
                .collect(Collectors.toList());
        }

        @Override
        public void visit(ExtractProcedure extractProcedure) {
            flatProcedures.add(extractProcedure);
            visitNext(extractProcedure);
        }

        @Override
        public void visit(TransformProcedure transformProcedure) {
            flatProcedures.add(transformProcedure);
            visitNext(transformProcedure);
        }

        @Override
        public void visit(LoadProcedure loadProcedure) {
            flatProcedures.add(loadProcedure);
            visitNext(loadProcedure);
        }

        @Override
        public void visit(QueryProcedure queryProcedure) {
            flatProcedures.add(queryProcedure);
            visitNext(queryProcedure);
        }

        @Override
        public void visit(DirectQueryProcedure queryProcedure) {
        }

    }
}
