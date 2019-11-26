package com.qihoo.qsql.codegen.flink;

import com.qihoo.qsql.codegen.ClassBodyComposer;
import com.qihoo.qsql.codegen.IntegratedQueryWrapper;
import com.qihoo.qsql.plan.proc.LoadProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;

/**
 * As a child of {@link IntegratedQueryWrapper}, {@link FlinkBodyWrapper} implement mixed operations code generation for
 * Flink.
 */
public class FlinkBodyWrapper extends IntegratedQueryWrapper {

    @Override
    public IntegratedQueryWrapper run(QueryProcedure plan) {
        return this;
    }

    @Override
    public void interpretProcedure(QueryProcedure plan) {
        plan.accept(new SimpleFlinkProcVisitor(composer));
    }

    @Override
    public void importSpecificDependency() {
        String[] imports = {
            "import org.apache.flink.api.common.typeinfo.BasicTypeInfo",
            "import org.apache.flink.api.common.typeinfo.TypeInformation",
            "import org.apache.flink.api.java.DataSet",
            "import org.apache.flink.api.java.ExecutionEnvironment",
            "import org.apache.flink.api.java.typeutils.RowTypeInfo",
            "import org.apache.flink.table.api.Table",
            "import org.apache.flink.table.api.TableEnvironment",
            "import org.apache.flink.table.api.java.BatchTableEnvironment",
            "import org.apache.flink.types.Row",
            "import org.apache.flink.table.api.Table",
            "import com.qihoo.qsql.exec.flink.FlinkRequirement"
        };
        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    public IntegratedQueryWrapper show() {
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, "tmp.print();\n");
        return this;
    }

    @Override
    public IntegratedQueryWrapper writeAsTextFile(String path, String deliminator) {
        return this;
    }

    @Override
    public IntegratedQueryWrapper writeAsJsonFile(String path) {
        return this;
    }

    @Override
    public void createTempTable(String tableName) {
        //TODO to implement
    }

    private class SimpleFlinkProcVisitor extends FlinkProcedureVisitor {

        SimpleFlinkProcVisitor(ClassBodyComposer composer) {
            super(composer);
        }

        @Override
        public void visit(LoadProcedure procedure) {
        }
    }
}
