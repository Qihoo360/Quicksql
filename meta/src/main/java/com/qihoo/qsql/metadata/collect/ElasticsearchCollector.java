package com.qihoo.qsql.metadata.collect;


import com.qihoo.qsql.metadata.collect.dto.ElasticsearchProp;
import com.qihoo.qsql.metadata.ColumnValue;
import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;

public class ElasticsearchCollector extends MetadataCollector {

    private ElasticsearchProp prop;
    private RestHighLevelClient highLevelClient;

    public ElasticsearchCollector(ElasticsearchProp prop, String filter) throws SQLException {
        super(filter);
        this.prop = prop;
        connect(prop);
    }

    private void connect(ElasticsearchProp prop) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(prop.getEsUser(), prop.getEsPass()));
        RestClientBuilder builder = RestClient.builder(new HttpHost(prop.getEsNodes(), prop.getEsPort()))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });

        highLevelClient = new RestHighLevelClient(builder);
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

    private Set<String> listIndexFromElastic(String regexp) throws IOException {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indices(regexp);
        GetAliasesResponse getAliasesResponse =  highLevelClient.indices().getAlias(request,RequestOptions.DEFAULT);
        Map<String, Set<AliasMetaData>> map = getAliasesResponse.getAliases();
        return map.keySet();
    }

    private List<ColumnValue> listFieldTypesFroElastic(String index) throws IOException {
        GetMappingsRequest getMappings = new GetMappingsRequest().indices(index);
        GetMappingsResponse getMappingResponse = highLevelClient.indices().getMapping(getMappings, RequestOptions.DEFAULT);
        Map<String, MappingMetaData> allMappings = getMappingResponse.mappings();
        assert allMappings.size() != 0;
        MappingMetaData indexMapping = allMappings.get(index);
        Map<String, Object> mappings = indexMapping.sourceAsMap();
        Map<String, Object> fields = (Map<String, Object>) mappings.get("properties");
        List<ColumnValue> columnValues = new ArrayList<>();
        fields.forEach((key,value) -> {
            ColumnValue columnValue = new ColumnValue();
            columnValue.setColumnName(key);
            Map<String,String> field = (Map<String,String>) value;
            columnValue.setTypeName(field.containsKey("type") ? field.get("type") : "keyword");
            columnValues.add(columnValue);
        });
        return columnValues;
    }
}
