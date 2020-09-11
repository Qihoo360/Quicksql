package com.qihoo.qsql.metadata.collect;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.qihoo.qsql.metadata.collect.dto.ElasticsearchProp;
import com.qihoo.qsql.metadata.ColumnValue;
import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import com.sun.tools.javac.util.Assert;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

public class ElasticsearchCollector extends MetadataCollector {

    private ElasticsearchProp prop;
    private RestClient restClient;
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * .
     */
    public ElasticsearchCollector(ElasticsearchProp prop, String filter) throws SQLException {
        super(filter);
        this.prop = prop;
        Map<String, Integer> coordinates = new HashMap<>();
        coordinates.put(prop.getEsNodes(), prop.getEsPort());
        Map<String, String> userConfig = new HashMap<>();
        userConfig.put("esUser", prop.getEsUser());
        userConfig.put("esPass", prop.getEsPass());
        this.restClient = connect(coordinates, userConfig);
    }

    @Override
    protected DatabaseValue convertDatabaseValue() {
        DatabaseValue value = new DatabaseValue();
        value.setDbType("es");
        value.setDesc("Who am I");
        value.setName(prop.getEsName());
        return value;
    }

    @Override
    protected List<DatabaseParamValue> convertDatabaseParamValue(Long dbId) {
        DatabaseParamValue[] paramValues = new DatabaseParamValue[6];
        for (int i = 0; i < paramValues.length; i++) {
            paramValues[i] = new DatabaseParamValue(dbId);
        }
        paramValues[0].setParamKey("esNodes").setParamValue(prop.getEsNodes());
        paramValues[1].setParamKey("esPort").setParamValue(Integer.toString(prop.getEsPort()));
        paramValues[2].setParamKey("esUser").setParamValue(prop.getEsUser());
        paramValues[3].setParamKey("esPass").setParamValue(prop.getEsPass());
        paramValues[4].setParamKey("esName").setParamValue(prop.getEsName());
        paramValues[5].setParamKey("esScrollNum").setParamValue("1");
        return Arrays.stream(paramValues).collect(Collectors.toList());
    }

    @Override
    protected TableValue convertTableValue(Long dbId, String tableName) {
        TableValue value = new TableValue();
        value.setTblName(tableName);
        value.setDbId(dbId);
        value.setCreateTime(new Date().toString());
        return value;
    }

    @Override
    protected List<ColumnValue> convertColumnValue(Long tbId, String tableName, String dbName) {
        try {
            List<ColumnValue> columns = listFieldTypesFroElastic(tableName);
            for (int i = 0; i < columns.size(); i++) {
                columns.get(i).setIntegerIdx(i + 1);
                columns.get(i).setComment("Who am I? 24601!!");
                columns.get(i).setCdId(tbId);
            }
            return columns;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    protected List<String> getTableNameList() {
        try {
            String regexp = filterRegexp.replaceAll("\\.", "\\.")
                .replaceAll("\\?", ".")
                .replaceAll("%", ".*");
            return new ArrayList<>(listIndexFromElastic(regexp));
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private static RestClient connect(Map<String, Integer> coordinates,
        Map<String, String> userConfig) {
        Objects.requireNonNull(coordinates, "coordinates");
        Preconditions.checkArgument(! coordinates.isEmpty(), "no ES coordinates specified");
        final Set<HttpHost> set = new LinkedHashSet<>();
        for (Map.Entry<String, Integer> entry : coordinates.entrySet()) {
            set.add(new HttpHost(entry.getKey(), entry.getValue()));
        }

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(userConfig.getOrDefault("esUser", "none"),
                userConfig.getOrDefault("esPass", "none")));

        return RestClient.builder(set.toArray(new HttpHost[0]))
            .setHttpClientConfigCallback(httpClientBuilder ->
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
            .build();
    }

    private Set<String> listIndexFromElastic(String regexp) throws IOException {
        final String endpoint = String.format("/_cat/indices?format=json&index=%s",regexp);
        final Response response = restClient.performRequest(new Request("GET", endpoint));
        Set<String> indexSet = new HashSet<>();
        try (InputStream is = response.getEntity().getContent()) {
            JsonNode root = mapper.readTree(is);
            Assert.check(root != null && root.size() > 0,"not found index");
            root.forEach(jsonNode -> {
                if (jsonNode.get("index") != null) {
                    indexSet.add(jsonNode.get("index").asText());
                }
            });
            return indexSet;
        }
    }

    private List<ColumnValue> listFieldTypesFroElastic(String index) throws IOException {
        final String endpoint = "/" + index + "/_mapping";
        final Response response = restClient.performRequest(new Request("GET", endpoint));
        try (InputStream is = response.getEntity().getContent()) {
            JsonNode root = mapper.readTree(is);
            if (! root.isObject() || root.size() != 1) {
                final String message = String.format(Locale.ROOT, "Invalid response for %s/%s "
                        + "Expected object of size 1 got %s (of size %d)", response.getHost(),
                    response.getRequestLine(), root.getNodeType(), root.size());
                throw new IllegalStateException(message);
            }

            JsonNode mappings = root.iterator().next().get("mappings");
            JsonNode typeObject = mappings.get("_doc");
            JsonNode properties = typeObject.get("properties");
            List<ColumnValue> columnValues = new ArrayList<>();
            properties.fieldNames().forEachRemaining(name -> {
                ColumnValue value = new ColumnValue();
                value.setComment("Who am I?");
                value.setColumnName(name);
                JsonNode node = properties.get(name).get("type");
                value.setTypeName(convertDataType(properties.get(name).has("type") ? node.asText() : "keyword"));
                columnValues.add(value);
            });
            return columnValues;
        }
    }

    private String convertDataType(String esType) {
        String type = esType.toLowerCase();
        switch (type) {
            case "integer":
            case "double":
            case "date":
            case "boolean":
            case "float":
                return type;
            case "text":
            case "keyword":
            case "ip":
                return "string";
            case "long":
                return "bigint";
            case "short":
                return "smallint";
            case "byte":
                return "tinyint";
            case "half_float":
            case "scaled_float":
                return "float";
            case "binary":
            case "object":
            case "nested":
                throw new RuntimeException("The current version does not support complex types");
            default:
                throw new IllegalStateException("Unknown type");
        }
    }
}
