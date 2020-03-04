package com.qihoo.qsql.codegen.spark;

import com.qihoo.qsql.codegen.ClassBodyComposer;
import com.qihoo.qsql.codegen.QueryGenerator;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.spark.SparkPipeline} is chosen and source
 * data of query is in MySql at the same time.
 */
public class SparkMongoGenerator extends QueryGenerator {

    @Override
    public void importDependency() {
        String[] imports = {
            "import org.apache.spark.sql.Dataset",
            "import org.apache.spark.sql.Row",
            "import java.util.Properties",
            "import com.qihoo.qsql.codegen.spark.SparkMongoGenerator",
            "import org.apache.spark.sql.SparkSession",
            "import org.apache.spark.SparkConf",
            "import org.apache.spark.api.java.JavaSparkContext",
            "import com.mongodb.spark.MongoSpark",
            "import org.bson.Document"
        };
        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    public void prepareQuery() {
    }

    //remember to remove temporary files after computing in hdfs or local machine
    @Override
    public void executeQuery() {
        //TODO change to generate invoking from reflection
        String invoked = String.format(
            "JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());"
                + "Dataset<Row> explicitDF = MongoSpark.load(jsc).toDF();");
        //,constructMongoUrl(properties),properties.getProperty("dbName"));
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invoked);
    }

    @Override
    public void saveToTempTable() {
        String created = "explicitDF.registerTempTable(\"" + tableName + "\");";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, created);
    }
}
