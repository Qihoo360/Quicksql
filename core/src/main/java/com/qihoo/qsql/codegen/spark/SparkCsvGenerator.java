package com.qihoo.qsql.codegen.spark;

import com.qihoo.qsql.codegen.ClassBodyComposer;
import com.qihoo.qsql.codegen.QueryGenerator;
import org.apache.commons.lang3.StringEscapeUtils;

public class SparkCsvGenerator extends QueryGenerator {

    @Override
    protected void importDependency() {
        String[] imports = {
            "import org.apache.spark.sql.Dataset",
            "import org.apache.spark.api.java.JavaRDD",
            "import org.apache.spark.sql.Row",
            "import org.apache.spark.sql.RowFactory",
            "import org.apache.spark.sql.SparkSession"
        };

        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    protected void prepareQuery() {
        // no action
    }

    @Override
    protected void executeQuery() {

        String invoked = "spark.read()\n"
            + "            .option(\"header\", \"true\")\n"
            + "            .option(\"inferSchema\", \"true\")\n"
            + "            .option(\"timestampFormat\", \"yyyy/MM/dd HH:mm:ss ZZ\")\n"
            + "            .option(\"delimiter\", \",\")\n"
            + "            .csv(\"" + StringEscapeUtils.escapeJava(properties.getProperty("directory")) + "\")\n"
            + "            .toDF()\n"
            + "            .createOrReplaceTempView(\"" + properties.getProperty("tableName") + "\");\n"
            + "        \n"
            + "      tmp = spark.sql(\"" + query.replaceAll("\\.", "_") + "\");";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invoked);
    }

    @Override
    public void saveToTempTable() {
        String created = "tmp.createOrReplaceTempView(\"" + tableName + "\");";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, created);
    }
}