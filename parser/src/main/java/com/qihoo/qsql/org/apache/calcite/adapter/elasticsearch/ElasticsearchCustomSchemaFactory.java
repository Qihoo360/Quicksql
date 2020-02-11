package com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.qihoo.qsql.org.apache.calcite.schema.Schema;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ElasticsearchCustomSchemaFactory extends ElasticsearchSchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name,
        Map<String, Object> operand) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        try {
            final Map<String, Integer> coordinates =
                mapper.readValue((String) operand.get("coordinates"),
                    new TypeReference<Map<String, Integer>>() {
                    });

            final Map<String, String> userConfig =
                mapper.readValue((String) operand.get("userConfig"),
                    new TypeReference<Map<String, String>>() {
                    });

            final String index = (String) operand.get("index");
            Preconditions.checkArgument(index != null, "index is missing in configuration");

            final RestClient client = connect(coordinates, userConfig);
            return new ElasticsearchSchema(client, index);
        } catch (IOException e) {
            throw new RuntimeException("Cannot parse values from json", e);
        }
    }

    /**
     * Builds elastic rest client from user configuration
     *
     * @param coordinates list of {@code hostname/port} to connect to
     * @return newly initialized low-level rest http client for ES
     */
    private static RestClient connect(Map<String, Integer> coordinates,
        Map<String, String> userConfig) {
        Objects.requireNonNull(coordinates, "coordinates");
        Preconditions.checkArgument(!coordinates.isEmpty(), "no ES coordinates specified");
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
            .setMaxRetryTimeoutMillis(300000).build();
    }

}
