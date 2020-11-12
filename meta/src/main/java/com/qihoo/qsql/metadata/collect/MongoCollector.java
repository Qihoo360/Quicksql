package com.qihoo.qsql.metadata.collect;


import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.qihoo.qsql.metadata.ColumnValue;
import com.qihoo.qsql.metadata.collect.dto.MongoPro;
import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

public class MongoCollector extends MetadataCollector {

    private MongoPro prop;
    private MongoDatabase mongoDatabase;

    /**
     * .
     */
    public MongoCollector(MongoPro prop, String filter) throws SQLException {
        super(filter);
        this.prop = prop;
        this.mongoDatabase = getDataBase(prop);
    }

    @Override
    protected DatabaseValue convertDatabaseValue() {
        DatabaseValue value = new DatabaseValue();
        value.setDbType("mongo");
        value.setDesc("Who am I");
        value.setName(prop.getDataBaseName());
        return value;
    }

    @Override
    protected List<DatabaseParamValue> convertDatabaseParamValue(Long dbId) {
        DatabaseParamValue[] paramValues = new DatabaseParamValue[7];
        for (int i = 0; i < paramValues.length; i++) {
            paramValues[i] = new DatabaseParamValue(dbId);
        }
        paramValues[0].setParamKey("host").setParamValue(prop.getHost());
        paramValues[1].setParamKey("port").setParamValue(Integer.toString(prop.getPort()));
        paramValues[2].setParamKey("dataBaseName").setParamValue(prop.getDataBaseName());
        paramValues[3].setParamKey("authMechanism").setParamValue(prop.getAuthMechanism());
        paramValues[4].setParamKey("userName").setParamValue(prop.getUserName());
        paramValues[5].setParamKey("password").setParamValue(prop.getPassword());
        paramValues[6].setParamKey("collectionName").setParamValue(prop.getCollectionName());
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
            List<ColumnValue> columns = getCollectionFields(tableName);
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
        if (StringUtils.isEmpty(filterRegexp)) {
            throw new RuntimeException("`Filter regular expression` needed to be set");
        }
        List<String> tableNames = null;
        if (null != mongoDatabase.listCollectionNames() && mongoDatabase.listCollectionNames().into(new
            ArrayList<String>()).contains(filterRegexp)) {
            tableNames = new ArrayList<>();
            tableNames.add(filterRegexp);
        }
        return tableNames;
    }

    private static MongoDatabase getDataBase(MongoPro mongoPro) {
        MongoClient mongoClient = null;
        if (StringUtils.isNotEmpty(mongoPro.getAuthMechanism())) {
            MongoCredential credentialOne = MongoCredential.createScramSha1Credential(mongoPro.getUserName(), mongoPro
                .getDataBaseName(), mongoPro.getPassword().toCharArray());
            mongoClient = new MongoClient(new ServerAddress(mongoPro.getHost(), mongoPro.getPort()),
                Arrays.asList(credentialOne));
        } else {
            mongoClient = new MongoClient(new ServerAddress(mongoPro.getHost(), mongoPro.getPort()));
        }
        return mongoClient.getDatabase(mongoPro.getDataBaseName());
    }


    private List<ColumnValue> getCollectionFields(String collectionName) throws IOException {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Set<String> fields = collection.find().first().keySet();
        List<ColumnValue> columnValues = new ArrayList<>();
        fields.stream().forEach(field -> {
            if (!field.equalsIgnoreCase("_id")) {
                ColumnValue value = new ColumnValue();
                value.setColumnName(field);
                Object obj = collection.find().first().get(field);
                String typeString = obj == null ? "Undefined" : obj.getClass().toString();
                value.setTypeName(convertDataType(typeString));
                columnValues.add(value);
            }
        });
        return columnValues;
    }

    private String convertDataType(String type) {
        switch (type) {
            case "class java.lang.Double":
                return "double";
            case "class java.lang.String":
                return "string";
            case "class java.lang.Boolean":
                return "boolean";
            case "class java.lang.Array":
                return "array";
            case "class java.lang.Object":
                return "object";
            default:
                return "undefined";
        }
    }
}
