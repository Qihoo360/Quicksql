package com.qihoo.qsql.api;

import com.google.gson.Gson;
import com.qihoo.qsql.api.SqlRunner.Builder;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.exec.JdbcPipeline;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.org.apache.calcite.rel.TreeNode;
import com.qihoo.qsql.plan.QueryProcedureProducer;
import com.qihoo.qsql.plan.QueryTables;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.utils.PropertiesReader;
import com.qihoo.qsql.utils.SqlUtil;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sql logical plan view.
 */
public class SqlLogicalPlanView {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlLogicalPlanView.class);
    private List<String> tableNames;
    protected Builder environment;


    SqlLogicalPlanView() {
        this.environment = SqlRunner.builder();
    }

    static {
        PropertiesReader.configLogger();
    }

    private static final String TEST_DATA_URL = PropertiesReader.getTestDataFilePath();

    /**
     * method of logical plan view.
     */
    public String getLogicalPlanView(String sql) {
        LOGGER.info("The sql being parser: \n" + sql);
        QueryTables tables = SqlUtil.parseTableName(sql);
        tableNames = tables.tableNames;
        environment.setTransformRunner(RunnerType.JDBC);

        LOGGER.debug("Parsed table names for upper SQL are: {}", tableNames);
        QueryProcedure procedure = createQueryPlan(sql);

        return getQueryProcedureLogicalView(procedure);
    }

    private QueryProcedure createQueryPlan(String sql) {
        String schema = environment.getSchemaPath();

        if (schema.isEmpty()) {
            LOGGER.info("Read schema from " + "embedded database.");
        } else {
            LOGGER.info("Read schema from " + ("manual schema, schema or path is: " + schema));
        }

        if (environment.getSchemaPath().isEmpty()) {
            schema = getFullSchemaFromAssetDataSource();
        }

        if (schema.equals("inline: ")) {
            schema = JdbcPipeline.CSV_DEFAULT_SCHEMA;
        }
        return new QueryProcedureProducer(schema, environment).createQueryProcedure(sql);
    }

    private String getFullSchemaFromAssetDataSource() {
        return "inline: " + MetadataPostman.getCalciteModelSchema(tableNames);
    }

    private String getQueryProcedureLogicalView(QueryProcedure queryProcedure) {
        TreeNode treeNode = queryProcedure.getTreeNode();
        Gson gson = new Gson();
        return gson.toJson(treeNode);
    }
}
