package com.qihoo.qsql.codegen.spark;

import com.qihoo.qsql.codegen.ClassBodyComposer;
import com.qihoo.qsql.codegen.QueryGenerator;
import com.qihoo.qsql.plan.proc.DirectQueryProcedure;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.LoadProcedure;
import com.qihoo.qsql.plan.proc.MemoryLoadProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.plan.proc.TransformProcedure;
import com.qihoo.qsql.plan.ProcedureVisitor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provide several visit methods to traversing the whole {@link QueryProcedure} which will be execute on Spark.
 */
public class SparkProcedureVisitor extends ProcedureVisitor {

    private ClassBodyComposer composer;
    private AtomicInteger varId;
    private String variable;

    public SparkProcedureVisitor(AtomicInteger varId, ClassBodyComposer composer) {
        this.composer = composer;
        this.varId = varId;
    }

    @Override
    public void visit(ExtractProcedure extractProcedure) {
        createVariableName();
        QueryGenerator queryBuilder = QueryGenerator.getQueryGenerator(
            extractProcedure, composer, variable, true);
        queryBuilder.execute();
        queryBuilder.saveToTempTable();
        visitNext(extractProcedure);
    }

    @Override
    public void visit(TransformProcedure transformProcedure) {
        createVariableName();
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "Dataset<Row> " + variable + " = spark.sql(\"" + transformProcedure.sql() + "\");");
        visitNext(transformProcedure);
    }

    @Override
    public void visit(LoadProcedure loadProcedure) {
        if (loadProcedure instanceof MemoryLoadProcedure) {
            composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
                variable + ".show();\n");
        }
        visitNext(loadProcedure);
    }

    @Override
    public void visit(DirectQueryProcedure queryProcedure) {
        visitNext(queryProcedure);
    }

    @Override
    public void visit(QueryProcedure queryProcedure) {
        visitNext(queryProcedure);
    }

    protected void createVariableName() {
        this.variable = "$" + (varId.incrementAndGet());
    }

}
