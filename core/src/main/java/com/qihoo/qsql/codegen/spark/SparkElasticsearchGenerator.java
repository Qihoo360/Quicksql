package com.qihoo.qsql.codegen.spark;

import com.qihoo.qsql.codegen.QueryGenerator;
import com.qihoo.qsql.codegen.ClassBodyComposer;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.spark.SparkPipeline} is chosen and source data of query is in
 * Elasticsearch at the same time.
 */
public class SparkElasticsearchGenerator extends QueryGenerator {

    private static final String LIMIT_PARAM = "qsql.limit";

    @Override
    public void importDependency() {
        String[] imports = {
            "import org.apache.spark.sql.Dataset",
            "import org.apache.spark.sql.Row",
            "import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL",
            "import org.apache.commons.lang.StringEscapeUtils",
            "import java.util.HashMap",
            "import java.util.Map",
            "import java.util.regex.Matcher",
            "import java.util.regex.Pattern"
        };

        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    public void prepareQuery() {
        composer
            .handleComposition(ClassBodyComposer.CodeCategory.METHOD, declareElasticsearchConfig());
    }

    @Override
    public void executeQuery() {
        Invoker config = Invoker.registerMethod("wrapElasticsearchConfig");
        String invokeWrap = config.invoke(convertProperties("esNodes", "esPort", "esUser",
            "esPass", "esIndex", "esQuery", "esScrollNum"));

        String tmpAlias = "_tmp";
        String invoked = "      Map<String, String> config =" + invokeWrap + ";\n"
            + "        Dataset<Row> " + alias + ";\n"
            + "        if(config.containsKey(\"" + LIMIT_PARAM + "\")){\n"
            + "            Dataset<Row> " + (alias + tmpAlias) + " = JavaEsSparkSQL.esDF(spark, \n\t\t\t\tconfig);\n"
            + "            " + alias + " = " + (alias + tmpAlias) + ".limit("
            + "Integer.parseInt(config.get(\"" + LIMIT_PARAM + "\")));\n"
            + "        }else{\n"
            + "            " + alias + " = JavaEsSparkSQL.esDF(spark \n\t\t\t\t, config);\n"
            + "        }";

        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invoked);
    }

    @Override
    public void saveToTempTable() {
        String created = alias + ".createOrReplaceTempView(\"" + tableName + "\");";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, created);
    }

    private String declareElasticsearchConfig() {
        return "       private Map<String, String> wrapElasticsearchConfig(\n"
            + "                                          String nodes, String port,\n"
            + "                                         String user, String password,\n"
            + "                                         String index, String json, String scrollNum) \n"
            + "       { \n"
            + "            String esQuery = json;\n"
            + "           if(!json.contains(\"\\\"query\\\":\")) {\n"
            + "              esQuery = json.substring(0, json.lastIndexOf(\"}\")) + \", \\\"query\\\":{}}\";\n"
            + "            }\n"
            + "           \n"
            + "        String pattern = \"\\\"_source\\\":\\\\[([\\\\d\\\\w,\\\"]*)\\\\]\";\n"
            + "        Pattern r = Pattern.compile(pattern);\n"
            + "        Matcher m = r.matcher(json);\n"
            + "        String esFields = \"\";\n"
            + "        while (m.find()) {\n"
            + "            esFields = m.group(1).replace(\"\\\"\", \"\");\n"
            + "        }\n"
            + "\n"
            + "        String pattern2 = \"\\\"size\\\":([\\\\d]*)\";\n"
            + "        Pattern r2 = Pattern.compile(pattern2);\n"
            + "        Matcher m2 = r2.matcher(json);\n"
            + "        int esLimit = 1;\n"
            + "        Map<String, String> config = new HashMap<String, String>();\n"
            + "        \n"
            + "        while (m2.find()) {\n"
            + "            config.put(\"" + LIMIT_PARAM + "\", m2.group(1));\n"
            + "            esLimit = Integer.parseInt(m2.group(1)) / Integer.parseInt(scrollNum);\n"
            + "        }\n"
            + "            config.put(\"es.nodes\", nodes);\n"
            + "            config.put(\"es.port\", port);\n"
            + "            config.put(\"es.query\", esQuery);\n"
            + "            config.put(\"es.mapping.date.rich\", \"false\");\n"
            + "            config.put(\"es.scroll.size\", \"20\");\n"
            + "            config.put(\"es.scroll.limit\", esLimit + \"\");\n"
            + "            config.put(\"es.net.http.auth.user\", user);\n"
            + "            config.put(\"es.net.http.auth.pass\", password);\n"
            + "            config.put(\"es.resource.read\", index);\n"
            + "            config.put(\"es.read.field.include\", esFields);\n"
            + "        \n"
            + "            return config;\n"
            + "        }\n";
    }
}
