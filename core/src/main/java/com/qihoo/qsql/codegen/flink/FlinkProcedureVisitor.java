package com.qihoo.qsql.codegen.flink;

import com.qihoo.qsql.codegen.ClassBodyComposer;
import com.qihoo.qsql.codegen.ClassBodyComposer.CodeCategory;
import com.qihoo.qsql.codegen.QueryGenerator;
import com.qihoo.qsql.plan.ProcedureVisitor;
import com.qihoo.qsql.plan.proc.DirectQueryProcedure;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.LoadProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.plan.proc.TransformProcedure;

/**
 * For traversing procedures to generate.
 */
public class FlinkProcedureVisitor extends ProcedureVisitor {

    private ClassBodyComposer composer;

    public FlinkProcedureVisitor(ClassBodyComposer composer) {
        this.composer = composer;
    }

    @Override
    public void visit(ExtractProcedure extractProcedure) {
        composer.handleComposition(CodeCategory.SENTENCE, "{");
        QueryGenerator builder = QueryGenerator.getQueryGenerator(
            extractProcedure, composer, false);
        builder.execute();
        builder.saveToTempTable();
        composer.handleComposition(CodeCategory.SENTENCE, "}");
        visitNext(extractProcedure);
    }

    //TODO Care for `tmp` is not declared.
    @Override
    public void visit(TransformProcedure transformProcedure) {
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "Table table = tableEnv.sqlQuery(\"" + transformProcedure.sql() + "\");");
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "tmp = tableEnv.toDataSet(table, Row.class);");
        visitNext(transformProcedure);
    }

    @Override
    public void visit(LoadProcedure loadProcedure) {
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, "tmp.print();\n");
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, "return null;\n");
        visitNext(loadProcedure);
    }

    @Override
    public void visit(QueryProcedure queryProcedure) {
        visitNext(queryProcedure);
    }

    @Override
    public void visit(DirectQueryProcedure queryProcedure) {
        visitNext(queryProcedure);
    }
}
