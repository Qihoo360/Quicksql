package com.qihoo.qsql.launcher;

import com.qihoo.qsql.api.DynamicSqlRunner;
import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.exception.EmptyMetadataException;
import com.qihoo.qsql.exception.QsqlException;
import com.qihoo.qsql.exec.AbstractPipeline;
import com.qihoo.qsql.exec.JdbcPipeline;
import com.qihoo.qsql.exec.result.CloseableIterator;
import com.qihoo.qsql.exec.result.JdbcResultSetIterator;
import com.qihoo.qsql.launcher.OptionsParser.SubmitOption;
import com.qihoo.qsql.metadata.MetadataMapping;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.metadata.SchemaAssembler;
import com.qihoo.qsql.plan.QueryProcedureProducer;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.utils.SqlUtil;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry class.
 */
public class ExecutionDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionDispatcher.class);

    static {
        config();
    }

    /**
     * for invoking by script out of project.
     *
     * @param args program arguments
     * @throws SQLException SQLException
     * @throws ParseException ParseException
     */
    public static void main(String[] args) throws
        SQLException, ParseException {
        if (args.length == 0) {
            throw new RuntimeException("I have nothing to execute,"
                + " Please input your SQL behind execution command as first argument!!");
        }

        OptionsParser parser = new OptionsParser(args);
        String sqlArg = parser.getOptionValue(SubmitOption.SQL);
        String runner = parser.getOptionValue(SubmitOption.RUNNER);

        String sql = new String(Base64.getDecoder().decode(sqlArg), StandardCharsets.UTF_8);
        List<String> tableNames = SqlUtil.parseTableName(sql);

        welcome();
        long latestTime = System.currentTimeMillis();
        LOGGER.info("Parsing table names has finished, {}",
            tableNames.isEmpty() ? "it's a non-table query."
                : "you will query tables: " + tableNames);

        if (tryToExecuteQueryDirectly(sql, tableNames, runner)) {
            System.out.printf("(%.2f sec)", ((double) (System.currentTimeMillis() - latestTime) / 1000));
            return;
        }

        String schema = loadSchemaForTables(tableNames);
        QueryProcedure procedure = new QueryProcedureProducer(schema).createQueryProcedure(sql);

        SqlRunner sqlRunner = SqlRunner.builder()
            .setAcceptedResultsNum(100)
            .setTransformRunner(RunnerType.value(runner)).ok();
        AbstractPipeline pipeline = ((DynamicSqlRunner) sqlRunner).chooseAdaptPipeline(procedure);
        if (pipeline instanceof JdbcPipeline && isPointedToExecuteByJdbc(runner)) {
            try (Connection connection = JdbcPipeline.createSpecificConnection(
                //TODO retrieve metadata repeatedly, should be optimized
                MetadataPostman.getAssembledSchema(tableNames))) {
                executeJdbcQuery(connection, sql);
                System.out.printf("(%.2f sec)", ((double) (System.currentTimeMillis() - latestTime) / 1000));
                return;
            }
        }

        LOGGER.info("It's a complex query, we need to setup computing engine, waiting...");

        ProcessExecClient execClient = ProcessExecClient.createProcessClient(pipeline, parser);
        execClient.exec();
        System.out.printf("(%.2f sec)\n", ((double) (System.currentTimeMillis() - latestTime) / 1000));
    }

    private static boolean tryToExecuteQueryDirectly(String sql, List<String> tableNames, String runner)
        throws SQLException {
        if (! isParsingCanBeIgnored(tableNames, runner)) {
            return false;
        }
        Connection connection = null;
        try {
            if (isNonTableQuery(tableNames)) {
                LOGGER.info("Connecting JDBC server, please wait a moment....");
                connection = JdbcPipeline.createCsvConnection();
            } else {
                List<SchemaAssembler> assemblers = MetadataPostman.getAssembledSchema(tableNames);
                if (! isSupportedJdbcDriver(assemblers)) {
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
                if (! iterator.hasNext()) {
                    System.out.println("[Empty Set]");
                }
                iterator.forEachRemaining(result ->
                    System.out.println(JdbcResultSetIterator.CONCAT_FUNC.apply(result)));
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
        return tableNames.size() <= 1 && isPointedToExecuteByJdbc(runner);
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
                + "Welcome to  \\___\\_\\__,_/_/\\___/_/|_|/____/\\___\\_\\/_____/  version 0.5.";
        String slogan = "   \\  Process data placed anywhere with the most flexible SQL  /";
        System.out.println(welcome);
        System.out.println(slogan);
    }

    private static void config() {
        String logProp;
        if (((logProp = System.getenv("QSQL_HOME")) != null) && ! logProp.isEmpty()) {
            PropertyConfigurator.configure(logProp
                + File.separator + "conf" + File.separator + "log4j.properties");
        }
    }
}
