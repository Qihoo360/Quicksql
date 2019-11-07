package com.qihoo.qsql.metadata;

import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get metadata and transfer it to json format.
 */
public class MetadataPostman {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataPostman.class);

    private MetadataPostman() {
    }

    /**
     * Fetching metadata from metadata store based on table names.
     *
     * @param identifiers Table names
     * @return List of SchemaAssembler
     */
    public static List<SchemaAssembler> getAssembledSchema(List<String> identifiers) {
        return identifiers.stream()
            .map(identifier -> {
                String[] names = identifier.split("\\.");
                MetadataFetcher fetcher;
                switch (names.length) {
                    case 1:
                        fetcher = new MetadataFetcher("", names[0]);
                        break;
                    case 2:
                        fetcher = new MetadataFetcher(names[0], names[1]);
                        break;
                    default:
                        throw new RuntimeException("Error sql identifier " + identifier);
                }
                return fetcher.transformSchemaFormat();
            })
            .collect(Collectors.toList());
    }

    /**
     * This method is used for assembling schema when the there is more than one tableName in Sql.
     *
     * @param tablesNames TableNames showed in Sql
     * @return List of SchemaAssembler
     */
    public static String getCalciteModelSchema(List<String> tablesNames) {
        List<SchemaAssembler> assembledSchema = getAssembledSchema(tablesNames);

        return assembleSchema(assembledSchema);
    }

    public static String assembleSchema(List<SchemaAssembler> assemblers) {
        return MetadataWrapper.assembleSchema(assemblers);
    }

    /**
     * Fetch metadata.
     */
    private static class MetadataFetcher {

        private String dbName;
        private String tableName;

        MetadataFetcher(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
        }

        private SchemaAssembler transformSchemaFormat() {
            Map<String, String> calciteProperties = new HashMap<>();
            try (MetadataClient client = new MetadataClient()) {
                List<TableValue> values = client.getTableSchema(tableName);

                TableValue theUniqueTable;
                List<DatabaseParamValue> params;
                DatabaseValue databaseValue;
                if (values.size() > 1) {
                    if (dbName.isEmpty()) {
                        throw new RuntimeException("Metadata for table '" + tableName
                            + "' is ambiguous!, please add concrete database name");
                    }

                    databaseValue = client.getBasicDatabaseInfo(dbName);
                    if (databaseValue == null) {
                        throw new RuntimeException("The database '" + dbName + "' was not found");
                    }

                    Long dbId = databaseValue.getDbId();
                    theUniqueTable = values.stream().filter(table -> table.getDbId().equals(dbId))
                        .findFirst().orElseThrow(() -> new RuntimeException(
                            "The table '" + tableName + "' for given database '" + dbName + "' was not found"));
                    params = client.getDatabaseSchema(dbId);
                } else if (values.size() == 1) {
                    theUniqueTable = values.get(0);
                    Long dbId = theUniqueTable.getDbId();
                    databaseValue = client.getBasicDatabaseInfoById(dbId);

                    if (databaseValue == null) {
                        throw new RuntimeException("The database '" + dbName + "' was not found");
                    }

                    client.getDatabaseSchema(dbId);
                    params = client.getDatabaseSchema(dbId);
                } else {
                    throw new RuntimeException("The table '" + tableName + "' not found in any database");
                }

                params.forEach(param -> calciteProperties.put(param.getParamKey(), param.getParamValue()));

                LOGGER.debug("Received connection info about table {} is {}", tableName, calciteProperties);

                List<ColumnValue> columnValues = client.getFieldsSchema(theUniqueTable.getTblId());

                LOGGER.debug("Received fields about table {} are {}", tableName, columnValues);

                String dbType = databaseValue.getDbType().toLowerCase();
                String dbName = databaseValue.getName();
                String tbName = theUniqueTable.getTblName();
                calciteProperties.put("dbName", dbName);
                calciteProperties.put("tableName", tbName);
                calciteProperties.put("dbType", dbType);
                MetadataMapping calciteMeta = MetadataMapping.convertToAdapter(dbType);
                calciteMeta.completeComponentProperties(calciteProperties);

                return new SchemaAssembler(dbName, tbName, calciteMeta, calciteProperties, columnValues);
            } catch (SQLException se) {
                throw new RuntimeException(se);
            }
        }
    }

    private static class MetadataWrapper {

        static String assembleSchema(List<SchemaAssembler> schemas) {
            if (schemas.isEmpty()) {
                return "";
            }

            schemas.sort(Comparator.comparing(x -> x.dbName));

            List<List<SchemaAssembler>> assemblerCollection = reduceSameSchema(schemas);

            String jsonSchemas = assemblerCollection.stream()
                .map(assemblers -> assemblers.get(0).reduceSameJsonSchema(assemblers))
                .reduce((left, right) -> left + ",\n" + right)
                .orElse("");

            return "{\n"
                + "  \"version\": \"1.0\",\n"
                + "  \"defaultSchema\": \"QSql\",\n"
                + "  \"schemas\": ["
                + jsonSchemas
                + "  \n]\n"
                + "}";
        }

        //try to use lambda here
        private static List<List<SchemaAssembler>> reduceSameSchema(List<SchemaAssembler> assemblers) {
            List<List<SchemaAssembler>> schemaCollections = new ArrayList<>();

            if (assemblers.size() == 1) {
                schemaCollections.add(new ArrayList<>(Collections.singletonList(assemblers.get(0))));
                return schemaCollections;
            }

            for (int i = 1, j = 0; i <= assemblers.size(); i++) {
                if (i == assemblers.size()) {
                    List<SchemaAssembler> list = new ArrayList<>();

                    for (int k = j; k < i; k++) {
                        list.add(assemblers.get(k));
                    }

                    schemaCollections.add(list);
                    return schemaCollections;
                }

                SchemaAssembler prev = assemblers.get(j);
                SchemaAssembler curr = assemblers.get(i);

                if (! prev.dbName.equals(curr.dbName)) {
                    List<SchemaAssembler> list = new ArrayList<>();

                    for (int k = j; k < i; k++) {
                        list.add(assemblers.get(k));
                    }

                    schemaCollections.add(list);
                    j = i;
                }
            }

            return schemaCollections;
        }

    }

}
