package com.qihoo.qsql.metadata.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qihoo.qsql.metadata.MetadataClient;
import com.qihoo.qsql.metadata.entity.ColumnValue;
import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.model.JsonCustomSchema;
import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.model.JsonRoot;
import org.apache.calcite.model.JsonTable;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataTransformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataTransformer.class);

    /**
     * Import metadata from json file to sqlite.
     *
     * @param args D
     */
    public static void main(String[] args) throws SQLException, ParseException {
        String path = MetadataTransformer.class.getClassLoader().getResource("QSql.json").getPath();
        // Option optionMetaJson = Option.builder().longOpt("json").hasArg().desc("metadata json").build();
        //
        // Options options = new Options();
        // options.addOption(optionMetaJson);
        //
        // CommandLineParser parser = new DefaultParser();

        // CommandLine commandLine = parser.parse(options, args);
        // if (commandLine.hasOption("json")) {
        //     String path = commandLine.getOptionValue("json");
        File file = new File(path);
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String json = reader.lines().reduce((x, y) -> x + y).orElse("");
            importMetadata(json);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        // } else {
        //     LOGGER.error("No transformable metadata json!! Please input with `json` or `file` option "
        //         + "then try again");
        // }
    }

    private static void importMetadata(String json) throws SQLException, IOException {
        MetadataClient client = null;
        try {
            client = new MetadataClient();
            client.setAutoCommit(false);
            JsonRoot root = new ObjectMapper().readValue(json, JsonRoot.class);
            MetadataClient finalClient = client;
            root.schemas.forEach(schema -> {
                JsonCustomSchema customSchema = ((JsonCustomSchema) schema);
                DatabaseValue dbValue = new DatabaseValue();
                dbValue.setName(customSchema.name);
                dbValue.setDesc("For test");
                dbValue.setDbType(getType(customSchema.factory));
                finalClient.insertBasicDatabaseInfo(dbValue);
                DatabaseValue withIdDbValue = finalClient.getBasicDatabaseInfo(customSchema.name);
                List<JsonTable> tables = customSchema.tables;
                tables.forEach(table -> {
                    JsonCustomTable customTable = ((JsonCustomTable) table);
                    List<DatabaseParamValue> params = customTable.operand.entrySet().stream()
                        .map(entry -> new DatabaseParamValue(
                            withIdDbValue.getDbId(),
                            entry.getKey(),
                            entry.getValue().toString())).collect(Collectors.toList());
                    if (finalClient.getDatabaseSchema(withIdDbValue.getDbId()).isEmpty()) {
                        finalClient.insertDatabaseSchema(params);
                    }
                    finalClient.insertTableSchema(new TableValue(withIdDbValue.getDbId(), table.name));

                    List<TableValue> tablesWithId = finalClient.getTableSchema(customTable.name);
                    if (tablesWithId.isEmpty()) {
                        throw new RuntimeException("ERROR about table parsed");
                    }
                    List<ColumnValue> values = customTable.columns.stream().map(column -> {
                        String[] names = column.name.split(":");
                        if (names.length != 2) {
                            throw new RuntimeException("ERROR about column parsed");
                        }
                        return new ColumnValue(tablesWithId.get(0).getTblId(), names[0], names[1]);
                    }).collect(Collectors.toList());
                    finalClient.insertFieldsSchema(values);
                });
            });
            client.commit();
        } catch (SQLException ex) {
            ex.printStackTrace();
            if (client != null) {
                client.rollback();
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static String getType(String factory) {
        if (factory.contains("Elasticsearch")) {
            return "es";
        } else if (factory.contains("Hive")) {
            return "hive";
        } else if (factory.contains("MySQL")) {
            return "mysql";
        } else {
            throw new RuntimeException("No given type!!");
        }
    }


}
