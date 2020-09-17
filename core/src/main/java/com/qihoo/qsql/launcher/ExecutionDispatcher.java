package com.qihoo.qsql.launcher;

import com.qihoo.qsql.api.DynamicSqlRunner;
import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.exception.EmptyMetadataException;
import com.qihoo.qsql.exception.QsqlException;
import com.qihoo.qsql.exec.AbstractPipeline;
import com.qihoo.qsql.exec.DdlFactory;
import com.qihoo.qsql.exec.DdlOperation;
import com.qihoo.qsql.exec.JdbcPipeline;
import com.qihoo.qsql.exec.result.CloseableIterator;
import com.qihoo.qsql.exec.result.JdbcPipelineResult;
import com.qihoo.qsql.exec.result.JdbcResultSetIterator;
import com.qihoo.qsql.launcher.OptionsParser.SubmitOption;
import com.qihoo.qsql.metadata.MetadataMapping;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.metadata.SchemaAssembler;
import com.qihoo.qsql.plan.QueryProcedureProducer;
import com.qihoo.qsql.plan.QueryTables;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.PreparedExtractProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.utils.PropertiesReader;
import com.qihoo.qsql.utils.SqlUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry class.
 */
public class ExecutionDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionDispatcher.class);
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    static {
        PropertiesReader.configLogger();
    }

    /**
     * for invoking by script out of project.
     *
     * @param args program arguments
     * @throws SQLException SQLException
     * @throws ParseException ParseException
     */
    public static void main(String[] args) {
        Date start = new Date();
        LOGGER.info("job.execute.start:" + SDF.format(start));
        try {
            if (args.length == 0) {
                throw new RuntimeException("I have nothing to execute,"
                    + " Please input your SQL behind execution command as first argument!!");
            }
            OptionsParser parser = new OptionsParser(args);
            final String sqlArg = parser.getOptionValue(SubmitOption.SQL);
            String runner = parser.getOptionValue(SubmitOption.RUNNER);

            String sql = new String(Base64.getDecoder().decode(sqlArg), StandardCharsets.UTF_8).trim();
            LOGGER.info("Your SQL is '{}'", sql);
            //list tables or databases from meta-data
            //if (sql.toUpperCase().startsWith("SHOW") || sql.toUpperCase().startsWith("DESC")) {
            //    //ShowDbHandler.dealSql(sql);
            //    return;
            //}
            DdlOperation ddlOperation = DdlFactory.getDdlOperation(getSqlType(sql));
            if (null != ddlOperation) {
                ddlOperation.execute(sql);
                return;
            }
            QueryTables tables = SqlUtil.parseTableName(sql);
            List<String> tableNames = tables.tableNames;

            if (tables.isDml()) {
                runner = "SPARK";
            }

            welcome();
            long latestTime = System.currentTimeMillis();
            LOGGER.info("Parsing table names has finished, {}",
                tableNames.isEmpty() ? "it's a non-table query."
                    : "you will query tables: " + tableNames);

            if (tryToExecuteQueryDirectly(sql, tableNames, runner)) {
                System.out.printf("(%.2f sec)", ((double) (System.currentTimeMillis() - latestTime) / 1000));
                return;
            }

            Builder builder = SqlRunner.builder()
                .setAcceptedResultsNum(100)
                .setTransformRunner(RunnerType.value(runner));

            String schema = loadSchemaForTables(tableNames);
            QueryProcedure procedure = new QueryProcedureProducer(schema, builder).createQueryProcedure(sql);

            AbstractPipeline pipeline = ((DynamicSqlRunner) builder.ok()).chooseAdaptPipeline(procedure);
            Date sqlParsed = new Date();
            LOGGER.info("SQL.parsed:" + SDF.format(sqlParsed));
            if (pipeline instanceof JdbcPipeline && isPointedToExecuteByJdbc(runner)) {
                jdbcExecuter(sql, tableNames, procedure, sqlParsed, start);
                return;
            }
            LOGGER.info("It's a complex query, we need to setup computing engine, waiting...");
            Date apply = new Date();
            LOGGER.info("apply.resource:" + SDF.format(apply));
            ProcessExecClient.createProcessClient(pipeline, parser, builder).exec();
            Date executed = new Date();
            LOGGER.info("job.execute.final:" + SDF.format(executed));
            LOGGER.info("SQL.parsed.time:" + String.valueOf(sqlParsed.getTime() - start.getTime()));
            LOGGER.info("resource.apply.time:" + String.valueOf(apply.getTime() - sqlParsed.getTime()));
            LOGGER.info("job.query.time:" + String.valueOf(executed.getTime() - apply.getTime()));
            System.exit(0);
        } catch (Exception ex) {
            ex.printStackTrace();
            LOGGER.error("execution.error:" + ex.getMessage());
            System.exit(1);
        }
    }

    private static void jdbcExecuter(String sql, List<String> tableNames, QueryProcedure procedure, Date sqlPased,
                                     Date start)
        throws
        SQLException {
        try (Connection connection = JdbcPipeline.createSpecificConnection(
            //TODO retrieve metadata repeatedly, should be optimized
            MetadataPostman.getAssembledSchema(tableNames))) {
            Date apply = new Date();
            LOGGER.info("apply.resource:" + SDF.format(apply));
            procedure = procedure.next();
            if (procedure instanceof PreparedExtractProcedure.ElasticsearchExtractor) {
                sql = ((PreparedExtractProcedure.ElasticsearchExtractor) procedure).sql();
            } else if (procedure instanceof PreparedExtractProcedure.MongoExtractor) {
                sql = ((PreparedExtractProcedure.MongoExtractor) procedure).sql();
            } else {
                sql = ((ExtractProcedure) procedure).toRecognizedQuery();
            }
            executeJdbcQuery(connection, sql);
            Date executed = new Date();
            LOGGER.info("job.execute.final:" + SDF.format(executed));
            // System.out.printf("(%.2f sec)", ((double) (System.currentTimeMillis() - latestTime) / 1000));
            LOGGER.info("SQL.parsed.time:" + String.valueOf(sqlPased.getTime() - start.getTime()));
            LOGGER.info("resource.apply.time:" + String.valueOf(apply.getTime() - sqlPased.getTime()));
            LOGGER.info("job.query.time:" + String.valueOf(executed.getTime() - apply.getTime()));
            return;
        }
    }

    private static boolean tryToExecuteQueryDirectly(String sql, List<String> tableNames, String runner)
        throws SQLException {
        if (!isParsingCanBeIgnored(tableNames, runner)) {
            return false;
        }
        Connection connection = null;
        try {
            if (isNonTableQuery(tableNames)) {
                LOGGER.info("Connecting JDBC server, please wait a moment....");
                connection = JdbcPipeline.createCsvConnection();
            } else {
                List<SchemaAssembler> assemblers = MetadataPostman.getAssembledSchema(tableNames);
                if (!isSupportedJdbcDriver(assemblers)) {
                    return false;
                }
                LOGGER.info("Connecting JDBC server, please wait a moment....");
                connection = JdbcPipeline.createSpecificConnection(assemblers);
            }
            executeJdbcQuery(connection, sql);
        } catch (SQLException ex) {
            throw new QsqlException("Error in building connection and executing sql: ", ex);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return true;
    }

    private static void executeJdbcQuery(Connection connection, String sql) {
        LOGGER.info("Jdbc connection has established, the result set is flying to you.");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            try (CloseableIterator<Object> iterator = new JdbcResultSetIterator<>(resultSet)) {
                new JdbcPipelineResult.ShowPipelineResult(iterator).print();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static boolean isSupportedJdbcDriver(List<SchemaAssembler> assemblers) {
        return assemblers.size() == 1
            && assemblers.get(0).getMetadataMapping() != MetadataMapping.Hive;
    }

    private static boolean isParsingCanBeIgnored(List<String> tableNames, String runner) {
        return tableNames.size() < 1 && isPointedToExecuteByJdbc(runner);
    }

    private static boolean isPointedToExecuteByJdbc(String runner) {
        return runner.equalsIgnoreCase("JDBC") || runner.equalsIgnoreCase("DYNAMIC");
    }

    private static boolean isNonTableQuery(List<String> tableNames) {
        return tableNames.isEmpty();
    }

    private static String loadSchemaForTables(List<String> tableNames) {
        if (tableNames.size() >= 1) {
            String schema = MetadataPostman.getCalciteModelSchema(tableNames);
            if (schema.isEmpty()) {
                throw new EmptyMetadataException("Table names [" + tableNames + "] cannot fetch metadata from "
                    + "metadata storage");
            }
            return "inline: " + schema;
        } else {
            return JdbcPipeline.CSV_DEFAULT_SCHEMA;
        }
    }

    private static void welcome() {
        String welcome =
            "               ____        _      __   _____ ____    __ \n"
                + "              / __ \\__  __(_)____/ /__/ ___// __ \\  / / \n"
                + "             / / / / / / / / ___/ //_/\\__ \\/ / / / / /  \n"
                + "            / /_/ / /_/ / / /__/ ,<  ___/ / /_/ / / /___\n"
                + "Welcome to  \\___\\_\\__,_/_/\\___/_/|_|/____/\\___\\_\\/_____/  version 0.7.0.";
        String slogan = "   \\  Process data placed anywhere with the most flexible SQL  /";
        System.out.println(welcome);
        System.out.println(slogan);
    }

    private static String getSqlType(String sql) {
        String sqlType = sql.split("\\s+")[0];
        return sqlType.toUpperCase();
    }
}
