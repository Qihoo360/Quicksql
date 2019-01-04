package com.qihoo.qsql.codegen.spark;

import com.qihoo.qsql.codegen.QueryGenerator;
import com.qihoo.qsql.codegen.ClassBodyComposer;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.spark.SparkPipeline} is chosen and when no table name is parsed
 * in query. For example, "select 1".
 */
public class SparkVirtualGenerator extends QueryGenerator {

    @Override
    protected void importDependency() {
        String[] imports = {
            "import org.apache.spark.sql.Dataset",
            "import org.apache.spark.sql.Row"
        };
        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    protected void prepareQuery() {
        //no action
    }

    @Override
    protected void executeQuery() {
        String invoked = "Dataset<Row> " + alias + " = spark.sql(\"" + query + "\");";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invoked);

    }

    @Override
    public void saveToTempTable() {
        String created = alias + ".createOrReplaceTempView(\"" + tableName + "\");";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, created);
    }

}
