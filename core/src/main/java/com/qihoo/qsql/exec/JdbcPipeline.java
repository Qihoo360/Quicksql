package com.qihoo.qsql.exec;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.qihoo.qsql.exception.QsqlException;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.metadata.SchemaAssembler;
import com.qihoo.qsql.plan.proc.DiskLoadProcedure;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.LoadProcedure;
import com.qihoo.qsql.plan.proc.PreparedExtractProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.exec.result.JdbcPipelineResult;
import com.qihoo.qsql.exec.result.JdbcResultSetIterator;
import com.qihoo.qsql.exec.result.PipelineResult;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.JsonCustomSchema;
import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.model.JsonRoot;
import org.apache.calcite.model.JsonSchema;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.runtime.FlatLists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * A pipeline special for Jdbc, that can concatenate all the steps in execution, including making
 * connection and statement, fetching result and printing in console.
 */
public class JdbcPipeline extends AbstractPipeline {

    public static final String CSV_DEFAULT_SCHEMA = "inline:"
        + "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'SALES',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'SALES',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.csv.CsvSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: 'sales'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcPipeline.class);
    private Connection connection;
    private Statement statement;
    private List<String> tableNames;


    public JdbcPipeline(QueryProcedure procedure,
        List<String> tableNames,
        SqlRunner.Builder builder) {
        super(procedure, builder);
        this.tableNames = tableNames;
    }

    /**
     * create Specific Connection based on Data Engine.
     *
     * @param json json of metadata config
     * @param parsedTables List of TableName
     * @return Connection
     */
    public static Connection createSpecificConnection(String json, List<String> parsedTables) {
        try {
            Map<String, String> properties = parseJsonSchema(parsedTables, json);
            switch (properties.get("type")) {
                case "jdbc":
                    LOGGER.debug("Connecting to JDBC server....");
                    return createJdbcConnection(properties);
                case "elasticsearch":
                    LOGGER.debug("Connection to Elasticsearch server....");
                    return createElasticsearchConnection(json);
                case "csv":
                    LOGGER.debug("Connection to CSV server....");
                    return createCsvConnection(json);
                default:
                    throw new RuntimeException("Not support");
            }
        } catch (IOException | ClassNotFoundException | SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * create Specific Connection based on Data Engine.
     *
     * @param assemblers List of SchemaAssembler
     * @return Connection
     */
    public static Connection createSpecificConnection(List<SchemaAssembler> assemblers) {
        if (assemblers.size() < 1) {
            try {
                return createCsvConnection();
            } catch (SQLException ex) {
                ex.printStackTrace();
                throw new QsqlException("Error when create Connection with non-table", ex);
            }
            // 将无表情况整合进入
            // throw new RuntimeException("There is no valid table name in sql!!");
        }
        SchemaAssembler result = assemblers.get(0);

        for (int i = 1; i < assemblers.size(); i++) {
            SchemaAssembler prev = assemblers.get(i - 1);
            SchemaAssembler curr = assemblers.get(i);
            checkConnectionInformation(prev, curr);
        }

        try {
            LOGGER.debug("Try to get connection infomation...");
            Map<String, String> conn = result.getConnectionProperties();
            LOGGER.debug("conn is : " + conn);
            switch (result.getMetadataMapping()) {
                case Elasticsearch:
                    LOGGER.debug("Connection to Elasticsearch server....");
                    return createElasticsearchConnection(
                        "inline: " + MetadataPostman.assembleSchema(assemblers));
                case JDBC:
                    LOGGER.debug("Connecting to JDBC server....");
                    return createJdbcConnection(conn);
                default:
                    throw new RuntimeException("Unsupported jdbc type");
            }
        } catch (ClassNotFoundException | SQLException ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    private static Connection createElasticsearchConnection(String json) throws SQLException {
        ConnectionFactory connectionFactory = new MapConnectionFactory(
            ImmutableMap.of("unquotedCasing", "unchanged", "caseSensitive", "true"),
            ImmutableList.of()
        ).with("model", json);

        Connection connection = connectionFactory.createConnection();
        LOGGER.debug("Connect with Elasticsearch server successfully!");
        return connection;
    }

    //TODO add zeroDateTimeBehavior=convertToNull property
    private static Connection createJdbcConnection(Map<String, String> conn)
        throws ClassNotFoundException, SQLException {
        if (! conn.containsKey("jdbcDriver")) {
            throw new RuntimeException("The `jdbcDriver` property needed to be set.");
        }
        Class.forName(conn.get("jdbcDriver"));
        // String ip = conn.getOrDefault("jdbcNode", "");
        // String port = conn.getOrDefault("jdbcPort", "");
        // String db = conn.getOrDefault("dbName", "");
        if (! conn.containsKey("jdbcUrl")) {
            throw new RuntimeException("The `jdbcUrl` property needed to be set.");
        }
        String url = conn.get("jdbcUrl");
        String user = conn.getOrDefault("jdbcUser", "");
        String password = conn.getOrDefault("jdbcPassword", "");
        Connection connection = DriverManager.getConnection(url, user, password);
        LOGGER.debug("Connect with MySQL server successfully!");
        return connection;
    }

    private static void checkConnectionInformation(SchemaAssembler prev, SchemaAssembler curr) {
        String message = "Query cross-engine tables should use mixed queries";
        assert prev.getMetadataMapping() == curr.getMetadataMapping() : message;
        assert prev.getConnectionProperties().getOrDefault("jdbcUrl", "")
            .equals(curr.getConnectionProperties().getOrDefault("jdbcUrl", ""));
    }

    private static Map<String, String> parseJsonSchema(List<String> names, String uri)
        throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);

        JsonRoot root;
        if (uri.startsWith("inline:")) {
            root = mapper.readValue(
                uri.substring("inline:".length()), JsonRoot.class);
        } else {
            root = mapper.readValue(new File(uri), JsonRoot.class);
        }

        JsonVisitor visitor = new JsonVisitor(names);
        visitor.visit(root);

        return visitor.getConnectionInfo();
    }

    /**
     * Create a simple csv connection for execute non-table query.
     *
     * @return the csv connection
     * @throws SQLException sql exception
     */
    public static Connection createCsvConnection(String json) throws SQLException {
        ConnectionFactory connectionFactory = new MapConnectionFactory(
            ImmutableMap.of("unquotedCasing", "unchanged", "caseSensitive", "true"),
            ImmutableList.of()
        ).with("model", json);

        // Properties info = new Properties();
        // info.put("model", json);
        Connection connection = connectionFactory.createConnection();
        LOGGER.debug("Connect with embedded calcite server successfully!");
        return connection;
    }

    public static Connection createCsvConnection() throws SQLException {
        return createCsvConnection(CSV_DEFAULT_SCHEMA);
    }

    @Override
    public void run() {
        QueryProcedure next = procedure.next();
        ResultSet resultSet = establishStatement();

        //TODO add jdbc sql translate
        if (next.hasNext() && next.next() instanceof DiskLoadProcedure) {
            String path = ((DiskLoadProcedure) next).path;
            String deliminator;
            if (((DiskLoadProcedure) next).getDataFormat() == LoadProcedure.DataFormat.DEFAULT) {
                deliminator = "\t";
            } else {
                deliminator = " ";
            }
            new JdbcPipelineResult.TextPipelineResult(
                new JdbcResultSetIterator<>(resultSet), path, deliminator).run();
        } else {
            new JdbcPipelineResult.ShowPipelineResult(
                new JdbcResultSetIterator<>(resultSet)).run();
        }
    }

    @Override
    public PipelineResult show() {
        ResultSet resultSet = establishStatement();
        return new JdbcPipelineResult.ShowPipelineResult(
            new JdbcResultSetIterator<>(resultSet));
    }

    @Override
    public PipelineResult asTextFile(String clusterPath, String deliminator) {
        ResultSet resultSet = establishStatement();
        return new JdbcPipelineResult.TextPipelineResult(
            new JdbcResultSetIterator<>(resultSet),
            clusterPath, deliminator);
    }

    @Override
    public PipelineResult asJsonFile(String clusterPath) {
        ResultSet resultSet = establishStatement();
        return new JdbcPipelineResult.JsonPipelineResult(
            new JdbcResultSetIterator<>(resultSet),
            clusterPath);
    }

    @Override
    public AbstractPipeline asTempTable(String tempTableName) {
        assert (procedure instanceof PreparedExtractProcedure.MySqlExtractor)
            : "Only support MySQL as temporary table";

        try {
            statement.execute("CREATE TEMPORARY TABLE " + tempTableName
                + " AS " + ((ExtractProcedure) procedure).toRecognizedQuery());
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return this;
    }

    @Override
    public void shutdown() {
        try {
            if (connection != null) {
                connection.close();
            }
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

    }

    private ResultSet establishStatement() {
        String sql;
        if (procedure instanceof PreparedExtractProcedure.ElasticsearchExtractor) {
            sql = ((PreparedExtractProcedure.ElasticsearchExtractor) procedure).sql();
        } else {
            sql = ((ExtractProcedure) procedure).toRecognizedQuery();
        }

        LOGGER.debug("Query sentence which is unparsed from logical plan is: \n\t{}", sql);

        try {
            connection = getConnection();

            if (connection instanceof CalciteConnection) {
                CalciteConnection calciteConnection = (CalciteConnection) connection;

                calciteConnection.getProperties().setProperty(
                    CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
                    Boolean.toString(false));

                calciteConnection.getProperties().setProperty(
                    CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
                    Boolean.toString(false));

                if (! calciteConnection.getProperties().containsKey(
                    CalciteConnectionProperty.TIME_ZONE.camelName())) {
                    calciteConnection.getProperties().setProperty(
                        CalciteConnectionProperty.TIME_ZONE.camelName(),
                        DateTimeUtils.UTC_ZONE.getID());
                }
            }

            statement = connection.createStatement();

            int limit = builder.getAcceptedResultsNum();
            int maxRowsLimit;
            if (limit <= 0) {
                maxRowsLimit = limit;
            } else {
                maxRowsLimit = Math.max(limit, 1);
            }
            statement.setMaxRows(maxRowsLimit);

            LOGGER.debug("Max rows limit is: {}", maxRowsLimit);

            return statement.executeQuery(sql);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Connection getConnection() {

        if (tableNames.isEmpty()) {
            try {
                LOGGER.debug("There is no table name in SQL, use embedded SQL connection");
                return createCsvConnection();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        if (builder.getSchemaPath().isEmpty()) {
            return createSpecificConnection(MetadataPostman.getAssembledSchema(tableNames));
        } else {
            return createSpecificConnection(builder.getSchemaPath(), tableNames);
        }
    }

    enum JdbcType {
        ELASTICSEARCH, JDBC, CSV,MONGO
    }

    public interface ConnectionPostProcessor {

        Connection apply(Connection connection) throws SQLException;
    }

    private abstract static class ConnectionFactory {

        public abstract Connection createConnection() throws SQLException;

        public ConnectionFactory with(String property, Object value) {
            throw new UnsupportedOperationException();
        }

        public ConnectionFactory with(ConnectionPostProcessor postProcessor) {
            throw new UnsupportedOperationException();
        }
    }

    private static class MapConnectionFactory extends ConnectionFactory {

        private final ImmutableMap<String, String> map;
        private final ImmutableList<ConnectionPostProcessor> postProcessors;

        private MapConnectionFactory(ImmutableMap<String, String> map,
            ImmutableList<ConnectionPostProcessor> postProcessors) {
            this.map = Preconditions.checkNotNull(map);
            this.postProcessors = Preconditions.checkNotNull(postProcessors);
        }

        @Override
        public Connection createConnection() throws SQLException {
            final Properties info = new Properties();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                info.setProperty(entry.getKey(), entry.getValue());
            }

            Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
            for (ConnectionPostProcessor postProcessor : postProcessors) {
                connection = postProcessor.apply(connection);
            }

            return connection;
        }

        @Override
        public ConnectionFactory with(String property, Object value) {
            return new MapConnectionFactory(
                FlatLists.append(this.map, property, value.toString()), postProcessors);
        }

        @Override
        public ConnectionFactory with(ConnectionPostProcessor postProcessor) {
            ImmutableList.Builder<ConnectionPostProcessor> builder = ImmutableList.builder();
            builder.addAll(postProcessors);
            builder.add(postProcessor);
            return new MapConnectionFactory(map, builder.build());
        }
    }

    //TODO modify jdbc to read from calcite jdbc connection, rather than to read from schema
    public static class JsonVisitor {

        private List<String> names;
        private List<Map<String, String>> jdbcProps = new ArrayList<>();
        private JdbcType type = null;

        JsonVisitor(List<String> names) {
            this.names = new ArrayList<>(names);
        }

        Map<String, String> getConnectionInfo() {
            Map<String, String> connectionInfo = new HashMap<>();
            switch (type) {
                case ELASTICSEARCH:
                    connectionInfo.put("type", "elasticsearch");
                    break;
                case CSV:
                    connectionInfo.put("type", "csv");
                    break;
                case JDBC:
                    connectionInfo =
                        jdbcProps.stream().reduce((left, right) -> {
                            String leftUrl = left.getOrDefault("jdbcUrl", "");
                            String rightUrl = right.getOrDefault("jdbcUrl", "");

                            if (leftUrl.substring(0, leftUrl.lastIndexOf("/")).equals(
                                rightUrl.substring(0, rightUrl.lastIndexOf("/")))) {
                                return left;
                            } else {
                                return Collections.emptyMap();
                            }
                        }).orElseThrow(() -> new RuntimeException(
                            "Not find any schema info for given table names in "
                                + "sql"));

                    connectionInfo.put("type", "jdbc");
                    break;
                default:
                    throw new RuntimeException("Do not support this engine type: " + type);
            }
            return connectionInfo;
        }

        void visit(JsonRoot jsonRoot) {
            for (JsonSchema schema : jsonRoot.schemas) {
                if (visit(schema)) {
                    break;
                }
            }
        }

        boolean visit(JsonSchema schema) {
            return schema instanceof JsonCustomSchema && visit((JsonCustomSchema) schema);
        }

        boolean visit(JsonCustomSchema schema) {
            Map<String, Map<String, String>> tableNames = new HashMap<>();

            List<JsonTable> tables = schema.tables;
            for (JsonTable table : tables) {
                Map<String, Object> map = ((JsonCustomTable) table).operand;
                Map<String, String> operand = map.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
                tableNames.put(((JsonCustomTable) table).name, operand);
                tableNames.put(schema.name + "." + ((JsonCustomTable) table).name, operand);
            }

            for (String part : names) {
                if (! tableNames.containsKey(part)) {
                    break;
                }
                jdbcProps.add(tableNames.get(part));
            }

            if (jdbcProps.size() == names.size()) {
                if (schema.factory.toLowerCase().contains("jdbc")) {
                    type = JdbcType.JDBC;
                } else if (schema.factory.toLowerCase().contains("elasticsearch")) {
                    type = JdbcType.ELASTICSEARCH;
                } else if (schema.factory.toLowerCase().contains("csv")) {
                    type = JdbcType.CSV;
                }
                return true;
            }
            return false;
        }
    }
}
