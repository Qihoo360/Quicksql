package com.qihoo.qsql.codegen.spark;

import com.qihoo.qsql.codegen.ClassBodyComposer;
import com.qihoo.qsql.codegen.QueryGenerator;
import java.util.Properties;
import org.apache.commons.lang.StringEscapeUtils;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.spark.SparkPipeline} is chosen and source
 * data of query is in MySql at the same time.
 */
public class SparkJdbcGenerator extends QueryGenerator {

    @Override
    public void importDependency() {
        String[] imports = {
            "import org.apache.spark.sql.Dataset",
            "import org.apache.spark.sql.Row",
            "import java.util.Properties",
            "import com.qihoo.qsql.codegen.spark.SparkJdbcGenerator"
        };
        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    public void prepareQuery() {}

    //remember to remove temporary files after computing in hdfs or local machine
    @Override
    public void executeQuery() {
        //TODO change to generate invoking from reflection
        Invoker config = Invoker.registerMethod("SparkJdbcGenerator.config");
        String invokeWrap = config.invoke(convertProperties("jdbcUser", "jdbcPassword", "jdbcDriver"));
        String invoked = String.format("tmp = spark.read().jdbc(\"%s\", \"%s\", %s);",
                properties.getOrDefault("jdbcUrl", ""),
            "(" + StringEscapeUtils.escapeJava(query) + ") " + tableName, invokeWrap);
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invoked);
    }

    @Override
    public void saveToTempTable() {
        String created = "tmp.createOrReplaceTempView(\"" + tableName + "\");";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, created);
    }

    /**
     * .
     */
    public static Properties config(String user, String password, String driver) {
        Properties properties = new Properties();
        properties.put("user", user);
        properties.put("password", password);
        properties.put("driver", driver);
        return properties;
    }
}
