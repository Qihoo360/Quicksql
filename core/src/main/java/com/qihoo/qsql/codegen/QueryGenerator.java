package com.qihoo.qsql.codegen;

import com.qihoo.qsql.codegen.flink.FlinkCsvGenerator;
import com.qihoo.qsql.codegen.flink.FlinkElasticsearchGenerator;
import com.qihoo.qsql.codegen.flink.FlinkHiveGenerator;
import com.qihoo.qsql.codegen.flink.FlinkMySqlGenerator;
import com.qihoo.qsql.codegen.flink.FlinkVirtualGenerator;
import com.qihoo.qsql.codegen.spark.SparkCsvGenerator;
import com.qihoo.qsql.codegen.spark.SparkElasticsearchGenerator;
import com.qihoo.qsql.codegen.spark.SparkHiveGenerator;
import com.qihoo.qsql.codegen.spark.SparkJdbcGenerator;
import com.qihoo.qsql.codegen.spark.SparkVirtualGenerator;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.PreparedExtractProcedure;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Code generator for different data source.
 */
//TODO remove context, there is no alias
public abstract class QueryGenerator {

    private static QueryGenerator elasticSearch = null;
    private static QueryGenerator hive = null;
    private static QueryGenerator jdbc = null;
    private static QueryGenerator virtual = null;
    private static QueryGenerator csv = null;

    protected ClassBodyComposer composer;
    protected String query;
    protected String tableName;
    protected Properties properties;

    protected QueryGenerator() {
    }

    /**
     * Get suitable QueryGenerator based on data source in query sql.
     *
     * @param procedure ExtractProcedure, also special for data source
     * @param composer Composer of class body
     * @param isSpark Decide if running engine is Spark
     * @return suitable QueryGenerator
     */
    public static QueryGenerator getQueryGenerator(ExtractProcedure procedure,
        ClassBodyComposer composer,
        boolean isSpark) {
        if (procedure instanceof PreparedExtractProcedure.HiveExtractor) {
            return createHiveQueryGenerator(procedure, composer, isSpark);
        } else if (procedure instanceof PreparedExtractProcedure.ElasticsearchExtractor) {
            return createElasticsearchQueryGenerator(procedure, composer, isSpark);
        } else if (procedure instanceof PreparedExtractProcedure.MySqlExtractor
            || procedure instanceof PreparedExtractProcedure.OracleExtractor) {
            return createJdbcQueryGenerator(procedure, composer, isSpark);
        } else if (procedure instanceof PreparedExtractProcedure.VirtualExtractor) {
            return createVirtualQueryGenerator(procedure, composer, isSpark);
        } else if (procedure instanceof PreparedExtractProcedure.CsvExtractor) {
            return createCsvQueryGenerator(procedure, composer, isSpark);
        } else {
            throw new RuntimeException("Unsupported Engine");
        }
    }

    private static QueryGenerator createHiveQueryGenerator(ExtractProcedure procedure,
        ClassBodyComposer composer,
        boolean isSpark) {
        if (hive == null) {
            if (isSpark) {
                hive = new SparkHiveGenerator();
            } else {
                hive = new FlinkHiveGenerator();
            }
            setSpecificState(hive, procedure, composer);
            hive.prepare();
        } else {
            setSpecificState(hive, procedure, composer);
        }
        return hive;
    }

    private static QueryGenerator createElasticsearchQueryGenerator(ExtractProcedure procedure,
        ClassBodyComposer composer,
        boolean isSpark) {
        if (elasticSearch == null) {
            if (isSpark) {
                elasticSearch =
                    new SparkElasticsearchGenerator();
            } else {
                elasticSearch =
                    new FlinkElasticsearchGenerator();
            }
            setSpecificState(elasticSearch, procedure, composer);
            elasticSearch.prepare();
        } else {
            setSpecificState(elasticSearch, procedure, composer);
        }
        return elasticSearch;
    }

    private static QueryGenerator createJdbcQueryGenerator(ExtractProcedure procedure,
        ClassBodyComposer composer,
        boolean isSpark) {
        if (jdbc == null) {
            if (isSpark) {
                jdbc = new SparkJdbcGenerator();
            } else {
                jdbc = new FlinkMySqlGenerator();
            }
            setSpecificState(jdbc, procedure, composer);
            jdbc.prepare();
        } else {
            setSpecificState(jdbc, procedure, composer);
        }
        return jdbc;
    }

    private static QueryGenerator createVirtualQueryGenerator(ExtractProcedure procedure,
        ClassBodyComposer composer,
        boolean isSpark) {
        if (virtual == null) {
            if (isSpark) {
                virtual = new SparkVirtualGenerator();
            } else {
                virtual = new FlinkVirtualGenerator();
            }
            setSpecificState(virtual, procedure, composer);
            virtual.prepare();
        } else {
            setSpecificState(virtual, procedure, composer);
        }
        return virtual;
    }

    private static QueryGenerator createCsvQueryGenerator(ExtractProcedure procedure,
        ClassBodyComposer composer,
        boolean isSpark) {
        if (csv == null) {
            if (isSpark) {
                csv = new SparkCsvGenerator();
            } else {
                csv = new FlinkCsvGenerator();
            }
            setSpecificState(csv, procedure, composer);
            csv.prepare();
        } else {
            setSpecificState(csv, procedure, composer);
        }
        return csv;
    }

    private static void setSpecificState(QueryGenerator generator,
        ExtractProcedure procedure,
        ClassBodyComposer composer) {
        generator.setComposer(composer);
        generator.setQuery(procedure.toRecognizedQuery());
        generator.setTableName(procedure.getTableName());
        generator.setProperties(procedure.getConnProperties());
    }

    /**
     * close each engine.
     */
    public static void close() {
        elasticSearch = null;
        hive = null;
        jdbc = null;
        virtual = null;
        csv = null;
    }

    //State Pattern
    private void setComposer(ClassBodyComposer composer) {
        this.composer = composer;
    }

    private void setQuery(String query) {
        this.query = query;
    }

    private void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private void setProperties(Properties properties) {
        this.properties = properties;
    }

    protected abstract void importDependency();

    protected abstract void prepareQuery();

    protected abstract void executeQuery();

    public abstract void saveToTempTable();

    public void execute() {
        executeQuery();
    }

    private void prepare() {
        importDependency();
        prepareQuery();
    }

    protected String[] convertProperties(String... params) {
        List<String> list = new ArrayList<>();
        for (String param : params) {
            list.add(getPropertyOrThrow(properties, param));
        }
        return list.toArray(new String[0]);
    }

    protected String with(String name, String alias) {
        return name + "_" + alias;
    }

    private String getPropertyOrThrow(Properties properties, String prop) {
        return Optional.ofNullable(properties.get(prop))
            .orElseThrow(() -> new RuntimeException("lack of " + prop + ", please check schema"))
            .toString();
    }

    protected abstract static class Invoker {

        String identifier;

        private Invoker(String identifier) {
            this.identifier = identifier;
        }

        public static Invoker registerMethod(String identifier) {
            return new MethodInvoker(identifier);
        }

        public String identifier() {
            return identifier;
        }

        public abstract String invoke(String... params);
    }

    protected static class MethodInvoker extends Invoker {
        private MethodInvoker(String identifier) {
            super(identifier);

        }

        @Override
        public String invoke(String... params) {
            String param = "(" + Arrays.stream(params)
                .map(p -> "\"" + p + "\"")
                .reduce((left, right) -> left + ", " + right)
                .orElse("") + ")";
            return identifier + param;
        }
    }
}
