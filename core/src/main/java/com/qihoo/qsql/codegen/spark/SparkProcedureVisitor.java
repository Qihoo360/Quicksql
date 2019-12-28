package com.qihoo.qsql.codegen.spark;

import com.qihoo.qsql.codegen.ClassBodyComposer;
import com.qihoo.qsql.codegen.ClassBodyComposer.CodeCategory;
import com.qihoo.qsql.codegen.QueryGenerator;
import com.qihoo.qsql.plan.ProcedureVisitor;
import com.qihoo.qsql.plan.proc.DirectQueryProcedure;
import com.qihoo.qsql.plan.proc.DiskLoadProcedure;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.LoadProcedure;
import com.qihoo.qsql.plan.proc.MemoryLoadProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.plan.proc.TransformProcedure;

/**
 * Provide several visit methods to traversing the whole {@link QueryProcedure} which will be execute on Spark.
 */
public class SparkProcedureVisitor extends ProcedureVisitor {

    private ClassBodyComposer composer;

    public SparkProcedureVisitor(ClassBodyComposer composer) {
        this.composer = composer;
    }

    @Override
    public void visit(ExtractProcedure extractProcedure) {
        composer.handleComposition(CodeCategory.SENTENCE, "{");
        QueryGenerator queryBuilder = QueryGenerator.getQueryGenerator(
            extractProcedure, composer, true);
        queryBuilder.execute();
        queryBuilder.saveToTempTable();
        composer.handleComposition(CodeCategory.SENTENCE, "}");
        visitNext(extractProcedure);
    }

    @Override
    public void visit(TransformProcedure transformProcedure) {
        String sql = transformProcedure.sql();
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "String sql = " + "\"" + sql + "\";");
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "tmp = spark.sql(sql);");
        visitNext(transformProcedure);
    }

    @Override
    public void visit(LoadProcedure loadProcedure) {
        if (loadProcedure instanceof MemoryLoadProcedure) {
            composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE);
        } else if (loadProcedure instanceof DiskLoadProcedure) {
            composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
                String.format("tmp.write().format(\"com.databricks.spark.csv\")"
                    + ".save(\"%s\");\n", ((DiskLoadProcedure) loadProcedure).path));
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
}
