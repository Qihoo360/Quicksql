package com.qihoo.qsql.api;

import com.qihoo.qsql.exception.QsqlException;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.plan.QueryProcedureProducer;
import com.qihoo.qsql.plan.proc.DirectQueryProcedure;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.PreparedExtractProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.exec.AbstractPipeline;
import com.qihoo.qsql.exec.JdbcPipeline;
import com.qihoo.qsql.exec.flink.FlinkPipeline;
import com.qihoo.qsql.exec.spark.SparkPipeline;
import com.qihoo.qsql.utils.SqlUtil;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main execution class of qsql task.
 * <p>
 * A sql will be parsed to get table names firstly when it is passed to this class. Then qsql will fetch related
 * metadata through {@link MetadataPostman} based on those table names. In the next step, {@link QueryProcedure} will be
 * created, which is the basic data structure and it can decide which {@link AbstractPipeline} should be used.
 * </p>
 * <p>
 * Certainly, a special {@link AbstractPipeline} can be chosen without executing such a complex process described above.
 * Consider that it may need more experience and practise instead, Dynamic RunnerType of {@link SqlRunner} choice is the
 * most recommended.
 * </p>
 */
public class DynamicSqlRunner extends SqlRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicSqlRunner.class);
    private AbstractPipeline pipeline = null;
    private List<String> tableNames;

    DynamicSqlRunner(Builder builder) {
        super(builder);
    }

    private String getFullSchemaFromAssetDataSource() {
        return "inline: " + MetadataPostman.getCalciteModelSchema(tableNames);
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

    @Override
    public AbstractPipeline sql(String sql) {
        LOGGER.info("The SQL that is ready to execute is: \n" + sql);
        tableNames = SqlUtil.parseTableName(sql);

        LOGGER.debug("Parsed table names for upper SQL are: {}", tableNames);
        QueryProcedure procedure = createQueryPlan(sql);

        LOGGER.debug("Created query plan, the complete plan is: \n{}",
            procedure.digest(new StringBuilder(), new ArrayList<>()));

        return chooseAdaptPipeline(procedure);
    }

    /**
     * Choose a suitable pipeline based on QueryProcedure.
     *
     * @param procedure QueryProcedure, which is created based on metadata
     * @return Suitable pipeline for the procedure
     */
    public AbstractPipeline chooseAdaptPipeline(QueryProcedure procedure) {
        //is single engine
        if (procedure instanceof DirectQueryProcedure
            && (environment.isDefaultMode() || environment.isJdbcMode())) {
            ExtractProcedure extractProcedure = (ExtractProcedure) procedure.next();

            LOGGER.debug("Choose specific runner {} to execute query",
                extractProcedure.getCategory());

            //specially for hive
            if (extractProcedure instanceof PreparedExtractProcedure.HiveExtractor) {
                if (environment.isJdbcMode()) {
                    throw new QsqlException("Hive cannot run in jdbc runner! Please use Spark runner instead");
                }
                return getOrCreateClusterPipeline(procedure);
            }

            //need a JDBC connection pool
            pipeline = new JdbcPipeline(extractProcedure, tableNames, environment);
            return pipeline;
        } else {
            if (LOGGER.isDebugEnabled()) {
                if (environment.isFlink()) {
                    LOGGER.debug("Choose mixed runner " + "Flink" + " to execute query");
                } else {
                    LOGGER.debug("Choose mixed runner " + "Spark" + " to execute query");
                }
            }
            return getOrCreateClusterPipeline(procedure);
        }
    }

    @Override
    public void stop() {
        LOGGER.info("Exiting runner...");
        if (pipeline != null) {
            pipeline.shutdown();
        }
        LOGGER.info("Exited runner, bye!!");
    }

    private AbstractPipeline getOrCreateClusterPipeline(QueryProcedure procedure) {
        if (this.pipeline == null) {
            //runner type can't be changed
            if (environment.isFlink()) {
                this.pipeline = new FlinkPipeline(procedure, environment);
            } else {
                this.pipeline = new SparkPipeline(procedure, environment);
            }
        }

        return this.pipeline;
    }
}
