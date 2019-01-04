package com.qihoo.qsql.plan;

import com.qihoo.qsql.exception.ParseException;
import com.qihoo.qsql.plan.proc.DataSetTransformProcedure;
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
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate the QueryProcedure chain.
 */
public class QueryProcedureProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryProcedureProducer.class);
    private FrameworkConfig config = null;

    /**
     * Constructs an QueryProcedureProducer with init planner config.
     *
     * @param jsonPath The url path of the metadata
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

        SubtreeSyncopator subtreeSyncopator = new SubtreeSyncopator(optimizedPlan);
        Map<RelNode, AbstractMap.SimpleEntry<String, RelOptTable>> resultRelNode =
            subtreeSyncopator.rootNodeSchemas;

        LoadProcedure procedure = new MemoryLoadProcedure();
        TransformProcedure transformProcedure =
            new DataSetTransformProcedure(procedure, optimizedPlan);

        List<ExtractProcedure> extractProcedures = new ArrayList<>();
        for (Map.Entry<RelNode, AbstractMap.SimpleEntry<String, RelOptTable>> entry :
            resultRelNode.entrySet()) {
            extractProcedures.add(PreparedExtractProcedure.createSpecificProcedure(
                transformProcedure, ((RelOptTableImpl) entry.getValue().getValue()),
                config, entry.getKey(),
                entry.getValue().getKey(), sql));
        }
        return new ProcedurePortFire(extractProcedures).optimize();
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
            .setCaseSensitive(true)
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
            SqlNode validated = planner.validate(parsed);
            return planner.rel(validated).rel;
        } catch (SqlParseException ex) {
            throw new ParseException("Error When Parsing Origin SQL: " + ex.getMessage(), ex);
        } catch (ValidationException | RelConversionException ev) {
            throw new ParseException("Error When Validating: " + ev.getMessage(), ev);
        } catch (Throwable ex) {
            throw new ParseException(
                "Unknown Parse Exception, Concrete Message is: " + ex.getMessage());
        }
    }

    private RelNode optimizeLogicalPlan(RelNode root) {
        final HepProgram program = new HepProgramBuilder()
            .addRuleInstance(SubQueryRemoveRule.PROJECT)
            .addRuleInstance(SubQueryRemoveRule.FILTER)
            .addRuleInstance(SubQueryRemoveRule.JOIN)
            .build();

        HepPlanner prePlanner = new HepPlanner(program);
        prePlanner.setRoot(root);

        return prePlanner.findBestExp();
    }
}
