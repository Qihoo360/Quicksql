package com.qihoo.qsql.metadata;

import com.qihoo.qsql.metadata.entity.ColumnValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Provide table related params and methods which can generate metadata Json based on those params.
 */
public class SchemaAssembler {

    public String dbName;
    private String tableName;
    private MetadataMapping factory;
    private Map<String, String> connProperties;
    private List<ColumnValue> fields;

    /**
     * Assemble schema.
     *
     * @param dbName Database name
     * @param tableName Table name
     * @param factory Schema factory and table factory based on storage type of table
     * @param connProperties Connection Properties of table
     * @param fields Column fields of table
     */
    public SchemaAssembler(String dbName,
        String tableName,
        MetadataMapping factory,
        Map<String, String> connProperties,
        List<ColumnValue> fields) {
        //Elasticsearch index separate by '-'
        this.dbName = dbName.replaceAll("-", "_");
        this.tableName = tableName;
        this.factory = factory;
        this.connProperties = connProperties;
        this.fields = fields;
    }

    public Map<String, String> getConnectionProperties() {
        return connProperties;
    }

    public MetadataMapping getMetadataMapping() {
        return factory;
    }

    /**
     * Reduce same Json schema if exists. Used when there are several tables in sql which are from one type of data
     * storage.
     *
     * @param sameSchemas Same schemas
     * @return Metadata json
     */
    public String reduceSameJsonSchema(List<SchemaAssembler> sameSchemas) {
        List<String> elements = new ArrayList<>();
        elements.add(formatPlainProperty("type", "custom"));
        elements.add(formatPlainProperty("name", dbName));
        elements.add(formatPlainProperty("factory", factory.schemaClass));

        if (factory == MetadataMapping.Elasticsearch) {
            elements.add(formatObjectProperty("operand", reduceJsonSchemaOperand()));
        }

        elements.add(formatArrayProperty("tables", reduceSameSchemaJsonTable(sameSchemas)));

        String schema = elements.stream().reduce((x, y) -> x + ",\n" + y).orElse("");

        return formatElementProperty(schema);
    }

    private String reduceSameSchemaJsonTable(List<SchemaAssembler> sameSchemas) {
        return sameSchemas.stream()
            .map(schema -> formatElementProperty(reduceJsonTable(schema)))
            .reduce((x, y) -> x + ",\n" + y)
            .orElse("");
    }

    //maybe exist same db name problem
    private String reduceJsonTable(SchemaAssembler schemaAssembler) {
        return Stream.of(
            formatPlainProperty("name", schemaAssembler.tableName),
            formatPlainProperty("factory", schemaAssembler.factory.tableClass),
            formatObjectProperty("operand",
                reduceJsonTableOperand(schemaAssembler.connProperties,
                    schemaAssembler.factory)),
            formatArrayProperty("columns", reduceJsonFields(schemaAssembler.fields))
        ).reduce((x, y) -> x + ",\n" + y).orElse("");
    }

    //maybe exist same db name problem
    private String reduceJsonSchemaOperand() {
        String coordinates = "{'" + connProperties.getOrDefault("esNodes", "")
            + "': " + connProperties.getOrDefault("esPort", "") + "}";
        String userConfig = "{'bulk.flush.max.actions': 10, " + "'bulk.flush.max.size.mb': 1,"
            + "'esUser':'" + connProperties.getOrDefault("esUser", "")
            + "'," + "'esPass':'" + connProperties.getOrDefault("esPass", "") + "'}";
        return Stream.of(
            formatPlainProperty("coordinates", coordinates),
            formatPlainProperty("userConfig", userConfig),
            formatPlainProperty("index", connProperties.getOrDefault("esIndex", "")
                .split("/")[0])
        ).reduce((x, y) -> x + ",\n" + y).orElse("");
    }

    private String reduceJsonTableOperand(Map<String, String> properties, MetadataMapping factory) {
        if (factory == MetadataMapping.Elasticsearch) {
            properties.put("dbName",
                properties.getOrDefault("dbName", "")
                    .replaceAll("-", "_"));
        }

        return factory.calciteProperties.stream()
            .map(prop -> formatPlainProperty(prop, properties.getOrDefault(prop, "")))
            .reduce((left, right) -> left + ",\n" + right)
            .orElse("");
    }

    //need to add more dataType
    private String reduceJsonFields(List<ColumnValue> fields) {
        return fields.stream()
            .filter(field -> ! (field.getColumnName().isEmpty() || field.getTypeName().isEmpty()))
            .map(this::convertFieldType)
            .reduce((left, right) -> left + ",\n" + right)
            .orElse("");
    }


    private String formatPlainProperty(String key, String value) {
        return "\"" + key + "\": " + "\"" + value  + "\"";
    }

    private String formatObjectProperty(String key, String value) {
        return "\"" + key + "\": " + "{\n" + value + "\n}";
    }

    private String formatElementProperty(String value) {
        return "{\n" + value + "\n}";
    }

    private String formatArrayProperty(String key, String value) {
        return "\"" + key + "\": " + "[\n" + value + "\n]";
    }

    private String convertFieldType(ColumnValue columnValue) {
        switch (columnValue.getTypeName().trim().toUpperCase()) {
            case "INT":
            case "INTEGER":
            case "STRING":
            case "VARCHAR":
            case "TINYINT":
            case "SMALLINT":
            case "BIGINT":
            case "FLOAT":
            case "DOUBLE":
            case "LONG":
            case "BOOLEAN":
            case "ARRAY":
            case "MAP":
            case "DATE":
            case "TIMESTAMP":
                return columnValue.toString();
            default:
                ColumnValue value = new ColumnValue();
                value.setColumnName(columnValue.getColumnName());
                value.setTypeName("STRING");
                return value.toString();
        }
    }
}
