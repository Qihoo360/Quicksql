package com.qihoo.qsql.codegen.spark;

import com.qihoo.qsql.codegen.ClassBodyComposer;
import com.qihoo.qsql.codegen.IntegratedQueryWrapper;
import com.qihoo.qsql.plan.proc.QueryProcedure;

/**
 * As a child of {@link IntegratedQueryWrapper}, {@link SparkBodyWrapper} implement mixed operations code generation for
 * Spark.
 */
public class SparkBodyWrapper extends IntegratedQueryWrapper {

    @Override
    public IntegratedQueryWrapper run(QueryProcedure plan) {
        plan.accept(new SparkProcedureVisitor(composer));
        return this;
    }

    @Override
    public void interpretProcedure(QueryProcedure plan) {
        plan.accept(new SimpleSparkProcVisitor(composer));
    }

    @Override
    public void importSpecificDependency() {
        String[] imports = {
            "import org.apache.spark.sql.SparkSession",
            "import com.qihoo.qsql.exec.Requirement",
            "import com.qihoo.qsql.exec.spark.SparkRequirement",
            "import org.apache.spark.sql.Dataset",
            "import org.apache.spark.sql.Row",
            "import java.util.Collections",
            "import java.util.List",
            "import java.util.Arrays",
            "import org.apache.spark.sql.Row",
            "import java.util.stream.Collectors",
            "import java.util.AbstractMap.SimpleEntry",
            "import java.util.Map",
            "import org.apache.spark.sql.catalyst.expressions.Attribute",
            "import scala.collection.JavaConversions;",
            "import org.apache.spark.sql.Row;"
        };
        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    public IntegratedQueryWrapper show() {
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "tmp.show();\n");
        getReturnNll();
        return this;
    }


    @Override
    public IntegratedQueryWrapper collect(int limit) {
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "List<Row> data = tmp.limit(" + limit + ").collectAsList();"
        );
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "List<Attribute> attributes = JavaConversions.seqAsJavaList(tmp.queryExecution().analyzed()"
                + ".output().seq());\n");
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "Map.Entry<List<?>,List<?>> result = new SimpleEntry<>(attributes,data);");
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "return result;");
        return this;
    }

    @Override
    public IntegratedQueryWrapper writeAsTextFile(String path, String deliminator) {
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "tmp.toJavaRDD().saveAsTextFile(\"" + path + "\");");
        getReturnNll();
        return this;
    }

    @Override
    public IntegratedQueryWrapper writeAsJsonFile(String path) {
        getReturnNll();
        return this;
    }

    @Override
    public void createTempTable(String tableName) {
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "tmp.createOrReplaceTempView(\"" + tableName + "\");");
    }

    private class SimpleSparkProcVisitor extends SparkProcedureVisitor {

        SimpleSparkProcVisitor(ClassBodyComposer composer) {
            super(composer);
        }
    }

    private void getReturnNll() {
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE,
            "return null;\n");
    }
}
