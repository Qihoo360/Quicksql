package com.qihoo.qsql.codegen.flink;

import com.qihoo.qsql.codegen.ClassBodyComposer;
import com.qihoo.qsql.codegen.QueryGenerator;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.flink.FlinkPipeline} is chosen and source data of query is in
 * Hive at the same time.
 */
public class FlinkHiveGenerator extends QueryGenerator {

    @Override
    protected void importDependency() {
        String[] imports = {
            "import org.apache.flink.api.java.DataSet",
            "import org.apache.flink.api.java.ExecutionEnvironment",
            "import org.apache.flink.table.api.Table",
            "import org.apache.flink.types.Row",
            "import com.qihoo.qsql.codegen.flink.FlinkHiveGenerator",
            "import org.apache.flink.table.catalog.hive.HiveCatalog"
        };
        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    protected void prepareQuery() {
        //no action
    }

    @Override
    protected void executeQuery() {
        String createVariable =
            "String name = \"default_catalog\";\nString version = \"1.2.1\";\nString hiveConfDir = "
                + "\"/hive/conf\";\n";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, createVariable);

        String invoked = String.format("HiveCatalog hiveCatalog = new HiveCatalog(%s, %s, %s, %s);",
            "name", "\"" + convertProperties("dbName")[0] + "\"",
            "hiveConfDir", "version");

        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invoked);
        String invokedSet = "tableEnv.registerCatalog(name, hiveCatalog);\n"
            + "        tableEnv.useCatalog(name);";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invokedSet);
        String registerTable =
            "Table table = tableEnv.sqlQuery(" + "\"" + query + "\"" + ");\ntableEnv.registerTable(\""
                + tableName
                + "\"," + "table);";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, registerTable);
    }

    @Override
    public void saveToTempTable() {

    }

}
