package com.qihoo.qsql.codegen.spark;

import com.qihoo.qsql.codegen.QueryGenerator;
import com.qihoo.qsql.codegen.ClassBodyComposer;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.spark.SparkPipeline} is chosen and source data of query is in
 * Hive at the same time.
 */
public class SparkHiveGenerator extends QueryGenerator {

    @Override
    public void importDependency() {
        String[] imports = {
            "import org.apache.spark.sql.Dataset",
            "import org.apache.spark.sql.Row"
        };
        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    public void prepareQuery() {
        //no action
    }

    @Override
    public void executeQuery() {
        String invoked = "Dataset<Row> " + alias + " = spark.sql(\"" + query + "\");";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invoked);
    }

    @Override
    public void saveToTempTable() {
        String created = alias + ".createOrReplaceTempView(\"" + tableName + "\");";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, created);
    }

}
