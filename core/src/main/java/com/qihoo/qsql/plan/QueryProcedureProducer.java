package com.qihoo.qsql.plan;

import com.qihoo.qsql.api.SqlRunner.Builder;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.exception.ParseException;
import com.qihoo.qsql.org.apache.calcite.adapter.mongodb.MongoTable;
import com.qihoo.qsql.org.apache.calcite.tools.YmlUtils;
import com.qihoo.qsql.plan.func.SqlRunnerFuncTable;
import com.qihoo.qsql.plan.proc.DataSetTransformProcedure;
import com.qihoo.qsql.plan.proc.DiskLoadProcedure;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.LoadProcedure;
import com.qihoo.qsql.plan.proc.MemoryLoadProcedure;
import com.qihoo.qsql.plan.proc.PreparedExtractProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.plan.proc.TransformProcedure;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import com.qihoo.qsql.org.apache.calcite.model.ModelHandler;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptTable;
import com.qihoo.qsql.org.apache.calcite.plan.RelTraitDef;
import com.qihoo.qsql.org.apache.calcite.plan.hep.HepPlanner;
import com.qihoo.qsql.org.apache.calcite.plan.hep.HepProgram;
import com.qihoo.qsql.org.apache.calcite.plan.hep.HepProgramBuilder;
import com.qihoo.qsql.org.apache.calcite.prepare.RelOptTableImpl;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.rules.FilterJoinRule.JoinConditionPushRule;
import com.qihoo.qsql.org.apache.calcite.rel.rules.SubQueryRemoveRule;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;
import com.qihoo.qsql.org.apache.calcite.sql.SqlIdentifier;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.ext.SqlInsertOutput;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParseException;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParser;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlConformanceEnum;
import com.qihoo.qsql.org.apache.calcite.sql2rel.SqlToRelConverter;
import com.qihoo.qsql.org.apache.calcite.tools.FrameworkConfig;
import com.qihoo.qsql.org.apache.calcite.tools.Frameworks;
import com.qihoo.qsql.org.apache.calcite.tools.Planner;
import com.qihoo.qsql.org.apache.calcite.tools.RelConversionException;
import com.qihoo.qsql.org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate the QueryProcedure chain.
 */
public class QueryProcedureProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryProcedureProducer.class);
    private FrameworkConfig config = null;
    private SqlRunnerFuncTable funcTable = SqlRunnerFuncTable.getInstance(RunnerType.DEFAULT);
    private Builder builder;
    private SqlNode output = null;

    /**
     * Constructs an QueryProcedureProducer with init planner config.
     *
     * @param jsonPath The url path of the metadata
     */
    public QueryProcedureProducer(String jsonPath, Builder builder) {
        this(jsonPath);
        this.builder = builder;
        //TODO rewrite by oo, include builder in sql runner
        if (builder.isSpark()) {
            this.funcTable = SqlRunnerFuncTable.getInstance(RunnerType.SPARK);
        }
    }

    /**
     * .
     */
    public QueryProcedureProducer(String jsonPath) {
        try {
            initPlannerConfig(jsonPath);
        } catch (IOException ex) {
            throw new ParseException("Error When Parsing Meta Data :" + ex.getMessage(), ex);
        }
    }

    /**
     * parse, validate, optimize, separate sql, and get query procedure.
     *
     * @param sql sql
     * @return QueryProcedure
     */
    public QueryProcedure createQueryProcedure(String sql) {
        RelNode originalLogicalPlan = buildLogicalPlan(sql);
        RelNode optimizedPlan = optimizeLogicalPlan(originalLogicalPlan);

        SubtreeSyncopator subtreeSyncopator = new SubtreeSyncopator(optimizedPlan, funcTable, builder);
        Map<RelNode, AbstractMap.SimpleEntry<String, RelOptTable>> resultRelNode =
            subtreeSyncopator.rootNodeSchemas;

        LoadProcedure procedure = createLoadProcedure();

        TransformProcedure transformProcedure =
            new DataSetTransformProcedure(procedure, subtreeSyncopator.getRoot());

        List<ExtractProcedure> extractProcedures = new ArrayList<>();
        for (Map.Entry<RelNode, AbstractMap.SimpleEntry<String, RelOptTable>> entry :
            resultRelNode.entrySet()) {
            extractProcedures.add(PreparedExtractProcedure.createSpecificProcedure(
                transformProcedure, ((RelOptTableImpl) entry.getValue().getValue()),
                config, entry.getKey(),
                entry.getValue().getKey(),
                (output instanceof SqlInsertOutput)
                    ? ((SqlInsertOutput) output).getSelect() : output, YmlUtils.getSourceMap()));
            //if table is mongo table and set connection information properties to builder object so as to set
            // parameters when do spark-submit job
            if (((RelOptTableImpl) entry.getValue().getValue()).getTable() instanceof MongoTable) {
                builder.setProperties(((MongoTable) ((RelOptTableImpl) entry.getValue().getValue()).getTable())
                    .getProperties());
            }
        }
        return new ProcedurePortFire(extractProcedures).optimize();
    }

    private LoadProcedure createLoadProcedure() {
        if (!(output instanceof SqlInsertOutput)) {
            return new MemoryLoadProcedure();
        }

        switch (((SqlInsertOutput) output).getDataSource().getSimple().toUpperCase()) {
            case "HDFS":
                SqlIdentifier path = (SqlIdentifier) ((SqlInsertOutput) output).getPath();
                if (path.names.size() != 1) {
                    throw new RuntimeException("Illegal path format, expected a simple path");
                }
                return new DiskLoadProcedure(path.names.get(0));
            default:
                throw new RuntimeException("Only support HDFS in this version.");
        }
    }

    private void initPlannerConfig(String jsonPath) throws IOException {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        new ModelHandler(rootSchema, jsonPath);
        final SqlToRelConverter.Config convertConfig = SqlToRelConverter.configBuilder()
            .withTrimUnusedFields(false)
            .withConvertTableAccess(false)
            .withExpand(false)
            .build();

        final SqlParser.Config parserConfig = SqlParser.configBuilder()
            .setConformance(SqlConformanceEnum.MYSQL_5)
            .setQuoting(Quoting.BACK_TICK)
            .setCaseSensitive(false)
            .setUnquotedCasing(Casing.UNCHANGED)
            .build();

        this.config = Frameworks.newConfigBuilder()
            .parserConfig(parserConfig)
            .defaultSchema(rootSchema)
            .traitDefs((List<RelTraitDef>) null)
            .sqlToRelConverterConfig(convertConfig)
            .build();
    }

    private RelNode buildLogicalPlan(String sql) {
        Planner planner = Frameworks.getPlanner(config);

        try {
            SqlNode parsed = planner.parse(sql);
            if (parsed instanceof SqlInsertOutput) {
                output = (SqlInsertOutput) parsed;
                parsed = ((SqlInsertOutput) parsed).getSelect();
            } else {
                output = parsed;
            }

            SqlNode validated = planner.validate(parsed);
            return planner.rel(validated).rel;
        } catch (SqlParseException ex) {
            throw new ParseException("Error When Parsing Origin SQL: " + ex.getMessage(), ex);
        } catch (ValidationException | RelConversionException ev) {
            throw new ParseException("Error When Validating: " + ev.getMessage(), ev);
        } catch (Throwable ex) {
            ex.printStackTrace();
            throw new ParseException(
                "Unknown Parse Exception, Concrete Message is: " + ex.getMessage(), ex);
        }
    }

    private RelNode optimizeLogicalPlan(RelNode root) {
        final HepProgram program = new HepProgramBuilder()
            .addRuleInstance(SubQueryRemoveRule.PROJECT)
            .addRuleInstance(SubQueryRemoveRule.FILTER)
            .addRuleInstance(SubQueryRemoveRule.JOIN)
            .addRuleInstance(JoinConditionPushRule.FILTER_ON_JOIN)
            .addRuleInstance(JoinConditionPushRule.JOIN)
            .build();

        HepPlanner prePlanner = new HepPlanner(program);
        prePlanner.setRoot(root);

        return prePlanner.findBestExp();
    }
}
