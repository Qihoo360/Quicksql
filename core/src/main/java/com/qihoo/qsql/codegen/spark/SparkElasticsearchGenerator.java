package com.qihoo.qsql.codegen.spark;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.qihoo.qsql.codegen.ClassBodyComposer;
import com.qihoo.qsql.codegen.QueryGenerator;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringEscapeUtils;


/**
 * Code generator, used when {@link com.qihoo.qsql.exec.spark.SparkPipeline} is chosen and source data of query is in
 * Elasticsearch at the same time.
 */
public class SparkElasticsearchGenerator extends QueryGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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
            "import java.util.regex.Pattern",
            "import com.qihoo.qsql.codegen.spark.SparkElasticsearchGenerator"
        };

        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    public void prepareQuery() {
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
    }

    //TODO remove alias
    @Override
    public void executeQuery() {
        decorateQuery(properties.getOrDefault("esQuery", "{}").toString());
        String dataSet = createDataSet();
        properties.put("esQuery", StringEscapeUtils.escapeJava(properties.getProperty("esQuery")));
        Invoker config = Invoker.registerMethod("SparkElasticsearchGenerator.config");
        String invokeWrap = config.invoke(convertProperties("esNodes", "esPort", "esUser",
            "esPass", "tableName", "esQuery", "esScrollNum"));
        String configure = String.format("Map<String, String> config = %s;", invokeWrap);
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, configure);
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, dataSet);
    }

    @Override
    public void saveToTempTable() {
        String created = "tmp.createOrReplaceTempView(\"" + tableName + "\");";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, created);
    }

    //    \"_source\":[\"city\",\"province\",\"digest\",\"type\",\"stu_id\"]}"
    private void decorateQuery(String json) {
        try {
            ObjectNode node = (ObjectNode) MAPPER.readTree(json);
            if (! node.has("query")) {
                ObjectNode query = MAPPER.createObjectNode();
                query.put("match_all", MAPPER.createObjectNode());
                node.put("query", query);
            }
            properties.put("esQuery", node.toString());

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private String createDataSet() {
        String dataSet = "tmp = JavaEsSparkSQL.esDF(spark, config)%s;";
        String scroll = properties.getProperty("esScrollNum");
        Integer scrollNum = scroll.isEmpty() ? 1 : Integer.parseInt(scroll);
        Optional<Integer> limit = getLimit();
        if (! limit.isPresent()) {
            return String.format(dataSet, "");
        }
        String tail = String.format(".limit(%d)", limit.get() / scrollNum);
        return String.format(dataSet, tail);
    }

    private Optional<Integer> getLimit() {
        String query = properties.getOrDefault("esQuery", "{}").toString();
        try {
            JsonNode node = MAPPER.readTree(query);
            return node.has("size") ? Optional.of(node.get("size").asInt()) : Optional.empty();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    //TODO extract to json generation

    /**
     * .
     */
    public static Map<String, String> config(String nodes, String port, String user, String password,
        String index, String json, String scrollNum) {
        //TODO remove it
        String pattern = "\"_source\":\\[([\\d\\w,\"]*)]";
        Pattern regex = Pattern.compile(pattern);
        Matcher matcher = regex.matcher(json);
        String esFields = "";
        while (matcher.find()) {
            esFields = matcher.group(1).replace("\"", "");
        }
        Map<String, String> config = new HashMap<>();
        config.put("es.nodes", nodes);
        config.put("es.port", port);
        config.put("es.query", json);
        config.put("es.mapping.date.rich", "false");
        config.put("es.scroll.size", "50");
        config.put("es.scroll.limit", "-1");
        config.put("es.net.http.auth.user", user);
        config.put("es.net.http.auth.pass", password);
        config.put("es.resource.read", index);
        config.put("es.read.field.include", esFields);

        return config;
    }
}
