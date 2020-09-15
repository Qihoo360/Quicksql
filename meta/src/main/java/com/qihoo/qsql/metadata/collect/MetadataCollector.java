package com.qihoo.qsql.metadata.collect;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qihoo.qsql.metadata.ColumnValue;
import com.qihoo.qsql.metadata.MetadataClient;
import com.qihoo.qsql.metadata.collect.dto.ElasticsearchProp;
import com.qihoo.qsql.metadata.collect.dto.HiveProp;
import com.qihoo.qsql.metadata.collect.dto.JdbcProp;
import com.qihoo.qsql.metadata.collect.dto.MongoPro;
import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import com.qihoo.qsql.org.apache.calcite.tools.YmlUtils;

import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MetadataCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataCollector.class);
    private static ObjectMapper mapper = new ObjectMapper();

    static {
        String logProp;
        if (((logProp = System.getenv("QSQL_HOME")) != null) && !logProp.isEmpty()) {
            PropertyConfigurator.configure(logProp
                + File.separator + "conf" + File.separator + "log4j.properties");
        }
    }

    String filterRegexp;
    private MetadataClient client = new MetadataClient();

    MetadataCollector(String filterRegexp) throws SQLException {
        this.filterRegexp = filterRegexp;
    }

    /**
     * .
     */
    public static MetadataCollector create(String json, String dataSource, String regexp) {
        try {
            LOGGER.info("Connecting server.....");
            dataSource = dataSource.toLowerCase();
            Map<String, Map<String, String>> sourceMap = YmlUtils.getSourceMap();
            if (sourceMap.containsKey(dataSource)) {
                String collectorClassName = sourceMap.get(dataSource).get("collectorClass");
                if ("hive".equals(collectorClassName)) {
                    return new HiveCollector(mapper.readValue(json, HiveProp.class), regexp);
                } else {
                    return new JdbcCollector(mapper.readValue(json, JdbcProp.class), regexp, sourceMap.get(dataSource),
                        dataSource);
                }
            }
            switch (dataSource.toLowerCase()) {
                case "es":
                case "elasticsearch":
                    return new ElasticsearchCollector(
                        mapper.readValue(json, ElasticsearchProp.class), regexp);
                case "mongo":
                    return new MongoCollector(
                        mapper.readValue(json, MongoPro.class), regexp) {
                    };
                default:
                    throw new RuntimeException("Unsupported datasource.");
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * entrance.
     */
    public static void main(String[] args) throws SQLException {
        if (args.length < 2) {
            throw new RuntimeException("Required conn info and type at least");
        }
        LOGGER.info("Input params: properties({}), type({}), filter regex({})",
            args[0], args[1], args[2]);
        MetadataCollector.create(args[0], args[1], args[2]).execute();
        System.exit(0);
    }

    /**
     * .
     */
    public void execute() throws SQLException {
        try {
            LOGGER.info("Connected successfully!!");
            client.setAutoCommit(false);
            DatabaseValue dbValue = convertDatabaseValue();
            Long dbId;
            DatabaseValue origin = client.getBasicDatabaseInfo(dbValue.getName());
            if (Objects.isNull(origin)) {
                dbId = client.insertBasicDatabaseInfo(dbValue);
                List<DatabaseParamValue> dbParams = convertDatabaseParamValue(dbId);
                client.insertDatabaseSchema(dbParams);
                LOGGER.info("Insert database {} successfully!!", dbValue.getName());
            } else {
                dbId = origin.getDbId();
                LOGGER.info("Reuse database {}!!", dbValue);
            }
            /// Query tableNames from schema
            List<String> tableNames = getTableNameList();
            tableNames.forEach(tableName -> {
                Long tbId;
                /// Tables already exist in schema, queried by matching tableName.
                List<TableValue> originTable = client.getTableSchema(tableName);
                if (originTable.stream().noneMatch(val -> val.getDbId().equals(dbId))) {
                    // any table is not related to ${dbId}
                    TableValue tableValue = convertTableValue(dbId, tableName);
                    tbId = client.insertTableSchema(tableValue);
                    LOGGER.info("Insert table {} successfully!!", tableValue.getTblName());
                    List<ColumnValue> cols = convertColumnValue(tbId, tableName, dbValue.getName());
                    if (cols.size() == 0) {
                        throw new RuntimeException("No column found in table '" + tableName + "'.");
                    }
                    client.insertFieldsSchema(cols);
                } else {
                    // some tables related to ${dbId}
                    TableValue shoot = originTable.stream()
                        .filter(val -> val.getDbId().equals(dbId)).findFirst()
                        .orElseThrow(() -> new RuntimeException("Query table error."));
                    tbId = shoot.getTblId();
                    LOGGER.info("Reuse table {}!!", shoot.getTblName());
                    client.deleteFieldsSchema(tbId);
                    LOGGER.info("Delete fields of table {}!!", shoot.getTblName());
                    List<ColumnValue> cols = convertColumnValue(tbId, tableName, dbValue.getName());
                    client.insertFieldsSchema(cols);
                }
            });
            client.commit();
            LOGGER.info("Successfully collected metadata for {} tables!!", tableNames.size());
            LOGGER.info(tableNames.stream().reduce((x, y) -> x + "\n" + y).orElse(""));
        } catch (SQLException ex) {
            client.rollback();
            LOGGER.info("Collect metadata failed!!");
        }
    }

    protected abstract DatabaseValue convertDatabaseValue();

    protected abstract List<DatabaseParamValue> convertDatabaseParamValue(Long dbId);

    protected abstract TableValue convertTableValue(Long dbId, String tableName);

    protected abstract List<ColumnValue> convertColumnValue(Long tbId, String tableName, String dbName);

    protected abstract List<String> getTableNameList();

}
