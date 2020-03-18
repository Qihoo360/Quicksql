package com.qihoo.qsql.plan.proc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.qihoo.qsql.org.apache.calcite.adapter.csv.CsvTable;
import com.qihoo.qsql.org.apache.calcite.adapter.custom.JdbcTable;
import com.qihoo.qsql.org.apache.calcite.adapter.druid.DruidQuery;
import com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch.ElasticsearchRel;
import com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch.ElasticsearchRules;
import com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch.ElasticsearchTable;
import com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch.ElasticsearchTranslatableTable;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.EnumerableConvention;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.EnumerableRel;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.EnumerableRules;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.JavaRowFormat;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.PhysType;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import com.qihoo.qsql.org.apache.calcite.adapter.hive.HiveTable;
import com.qihoo.qsql.org.apache.calcite.adapter.mongodb.MongoTable;
import com.qihoo.qsql.org.apache.calcite.adapter.virtual.VirtualTable;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptCluster;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptLattice;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptMaterialization;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptPlanner;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptTable;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptUtil;
import com.qihoo.qsql.org.apache.calcite.plan.RelTraitSet;
import com.qihoo.qsql.org.apache.calcite.prepare.Prepare;
import com.qihoo.qsql.org.apache.calcite.prepare.RelOptTableImpl;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.RelVisitor;
import com.qihoo.qsql.org.apache.calcite.rel.core.TableScan;
import com.qihoo.qsql.org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.sql.SqlDialect;
import com.qihoo.qsql.org.apache.calcite.sql.SqlDialect.Context;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.dialect.CalciteSqlDialect;
import com.qihoo.qsql.org.apache.calcite.sql.dialect.HiveSqlDialect;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParseException;
import com.qihoo.qsql.org.apache.calcite.tools.FrameworkConfig;
import com.qihoo.qsql.org.apache.calcite.tools.Frameworks;
import com.qihoo.qsql.org.apache.calcite.tools.Planner;
import com.qihoo.qsql.org.apache.calcite.tools.Program;
import com.qihoo.qsql.org.apache.calcite.tools.Programs;
import com.qihoo.qsql.org.apache.calcite.tools.RelConversionException;
import com.qihoo.qsql.org.apache.calcite.tools.RuleSet;
import com.qihoo.qsql.org.apache.calcite.tools.RuleSets;
import com.qihoo.qsql.org.apache.calcite.tools.ValidationException;
import com.qihoo.qsql.org.apache.calcite.util.Pair;
import com.qihoo.qsql.org.apache.calcite.util.Util;
import com.qihoo.qsql.utils.SqlUtil;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.AbstractList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Create specific {@link ExtractProcedure} based on type of {@link RelOptTable}, which represent extracting data in
 * different data source.
 */
public abstract class PreparedExtractProcedure extends ExtractProcedure {

    //Logical plan tree
    protected RelNode relNode;
    private FrameworkConfig config;

    private PreparedExtractProcedure(
        QueryProcedure next,
        Properties properties,
        FrameworkConfig config,
        RelNode relNode,
        String tableName) {
        super(next, properties, tableName);
        this.config = config;
        this.relNode = relNode;
    }

    /**
     * create specific Extractor according to class of relOptTable.
     *
     * @param next next procedure in DAG
     * @param relOptTable relOptTable in procedure
     * @param config config of procedure
     * @param relNode relNode of Procedure
     * @param tableName tableName of Sql
     */
    public static PreparedExtractProcedure createSpecificProcedure(
        QueryProcedure next,
        RelOptTableImpl relOptTable,
        FrameworkConfig config,
        RelNode relNode,
        String tableName,
        SqlNode sqlNode,
        Map<String, Map<String,String>> jdbcSources) {
        //rewrite this paragraph
        if (relOptTable.getTable() instanceof ElasticsearchTable) {
            String newSql = Util.toLinux(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql());
            return new ElasticsearchExtractor(next,
                ((ElasticsearchTranslatableTable) relOptTable.getTable()).getProperties(),
                config, relNode, tableName, newSql);
        } else if (relOptTable.getTable() instanceof HiveTable) {
            return new HiveExtractor(next,
                ((HiveTable) relOptTable.getTable()).getProperties(),
                config, relNode, tableName);
        } else if (relOptTable.getTable() instanceof MongoTable) {
            String newSql = Util.toLinux(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql());
            return new MongoExtractor(next, ((MongoTable) relOptTable.getTable())
                .getProperties(), config, relNode, tableName, newSql);
        } else if (relOptTable.getTable() instanceof JdbcTable) {
            //TODO add more jdbc type
            String dbType = ((JdbcTable) relOptTable.getTable())
                .getProperties().getProperty("dbType", "unknown");

            if (!jdbcSources.keySet().contains(dbType)) {
                throw new RuntimeException("Unsupported database type");
            }
            if (MapUtils.isEmpty(jdbcSources.get(dbType))) {
                throw new RuntimeException("Not found params");
            }
            return new JdbcExtractor(next, ((JdbcTable) relOptTable.getTable())
                .getProperties(), config, relNode, tableName,dbType, jdbcSources.get(dbType));

        } else if (relOptTable.getTable() instanceof VirtualTable) {
            String newSql = Util.toLinux(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql());
            return new VirtualExtractor(next,
                ((VirtualTable) relOptTable.getTable()).getProperties(),
                config, relNode, tableName, newSql);
        } else if (relOptTable.getTable() instanceof CsvTable) {
            String newSql = Util.toLinux(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql());
            return new CsvExtractor(next,
                ((CsvTable) relOptTable.getTable()).getProperties(),
                config, relNode, tableName, newSql);
        } else {
            throw new RuntimeException("Unsupported metadata type");
        }
    }

    private static RelNode toPhysicalPlan(RelNode root, RuleSet rules) {
        Program program = Programs.of(rules);
        RelOptPlanner plan = root.getCluster().getPlanner();
        RelTraitSet traits = plan.emptyTraitSet().replace(EnumerableConvention.INSTANCE);

        return program.run(plan, root, traits,
            ImmutableList.<RelOptMaterialization>of(),
            ImmutableList.<RelOptLattice>of());
    }

    protected String sql(SqlDialect dialect) {
        SqlNode sqlNode = new RelToSqlConverter(dialect).visitChild(0, relNode).asStatement();
        return Util.toLinux(sqlNode.toSqlString(dialect).getSql()).replaceAll("\n", " ");
    }

    @Override
    public String toString() {
        return toRecognizedQuery();
    }

    @Override
    public StringBuilder digest(StringBuilder builder, List<String> tabs) {
        String prefix = tabs.stream().reduce((x, y) -> x + y).orElse("");
        tabs.add("\t");
        StringBuilder newBuilder = builder.append(prefix).append("[ExtractProcedure]")
            .append("\n").append(prefix)
            .append(" \"type\":").append(getCategory())
            .append("\n").append(prefix)
            .append(" \"conn_info\":").append(getConnProperties())
            .append("\n").append(prefix)
            .append(" \"logical_node\":").append(relNode)
            .append("\n");

        if (next() != null) {
            return next().digest(newBuilder, tabs);
        } else {
            return newBuilder;
        }
    }

    public abstract static class NoSqlExtractor extends PreparedExtractProcedure {

        NoSqlExtractor(QueryProcedure next, Properties properties,
                       FrameworkConfig config, RelNode relNode, String tableName) {
            super(next, properties, config, relNode, tableName);
        }

        RelNode createLogicalPlan(String sql) {
            Planner planner = Frameworks.getPlanner(super.config);

            try {
                SqlNode parsed = planner.parse(sql);
                SqlNode validated = planner.validate(parsed);
                return planner.rel(validated).rel;
            } catch (SqlParseException
                | ValidationException
                | RelConversionException ex) {
                throw new RuntimeException(ex.getMessage());
            }
        }
    }

    /**
     * ElasticsearchExtractor.
     */
    public static class ElasticsearchExtractor extends NoSqlExtractor {

        public Properties properties;
        private String sql;

        /**
         * Extractor of Elasticsearch.
         *
         * @param next next procedure in DAG
         * @param properties properties of Procedure
         * @param config config of Procedure
         * @param relNode relNode
         * @param tableName tableName in Sql
         * @param sql sql
         */
        public ElasticsearchExtractor(QueryProcedure next, Properties properties,
                                      FrameworkConfig config, RelNode relNode,
                                      String tableName, String sql) {
            super(next, properties, config, relNode, tableName);
            this.sql = sql;
            this.properties = properties;
        }

        @Override
        public String toRecognizedQuery() {
            final RuleSet rules = RuleSets.ofList(
                EnumerableRules.ENUMERABLE_PROJECT_RULE,
                EnumerableRules.ENUMERABLE_FILTER_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                EnumerableRules.ENUMERABLE_SORT_RULE,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE
            );

            RelNode esLogicalPlan = createLogicalPlan(sql(
                new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT)));
            RelNode esPhysicalPlan = toPhysicalPlan(esLogicalPlan, rules);
            String esJson = toElasticsearchQuery((EnumerableRel) esPhysicalPlan);
            //TODO debug toLowerCase
            properties.put("esQuery", esJson);
            return esJson;
        }

        @Override
        public String getCategory() {
            return "Elasticsearch";
        }

        private String toElasticsearchQuery(EnumerableRel root) {
            try {
                EnumerableRelImplementor relImplementor =
                    new EnumerableRelImplementor(root.getCluster().getRexBuilder(),
                        ImmutableMap.of());
                final ElasticsearchRel.Implementor elasticsearchImplementor =
                    new ElasticsearchRel.Implementor();
                RelDataType rowType = root.getRowType();
                final PhysType physType = PhysTypeImpl.of(relImplementor.getTypeFactory(), rowType,
                    Prefer.ARRAY.prefer(JavaRowFormat.ARRAY));

                List<Pair<String, Class>> pairs = Pair
                    .zip(ElasticsearchRules.elasticsearchFieldNames(rowType),
                        new AbstractList<Class>() {
                            @Override
                            public Class get(int index) {
                                return physType.fieldClass(index);
                            }

                            @Override
                            public int size() {
                                return rowType.getFieldCount();
                            }
                        });

                return elasticsearchImplementor.convert(root.getInput(0), pairs);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                Prepare.CatalogReader.THREAD_LOCAL.remove();
            }
        }

        public String sql() {
            return sql;
        }
    }

    public static class MongoExtractor extends NoSqlExtractor {
        private String sql;

        /**
         * Extractor of Mongodb.
         *
         * @param next next procedure in DAG
         * @param properties properties of Procedure
         * @param config config of Procedure
         * @param relNode relNode
         * @param tableName tableName in Sql
         * @param sql sql
         */
        public MongoExtractor(QueryProcedure next, Properties properties,
                              FrameworkConfig config, RelNode relNode,
                              String tableName, String sql) {
            super(next, properties, config, relNode, tableName);
            this.sql = sql;
        }

        @Override
        public String toRecognizedQuery() {
            return sql(new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT)).toLowerCase();
        }

        @Override
        public String getCategory() {
            return "Mongo";
        }

        public String sql() {
            return sql;
        }
    }

    public static class DruidExtractor extends NoSqlExtractor {

        private String sql;

        public DruidExtractor(QueryProcedure next, Properties properties,
                              FrameworkConfig config, RelNode relNode, String tableName, String sql) {
            super(next, properties, config, relNode, tableName);
            this.sql = sql;
        }

        @Override
        public String toRecognizedQuery() {
            RelNode druidLogicalPlan = createLogicalPlan(
                sql(new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT)));
            RelNode druidPhysicalPlan = transformToDruidPlan(druidLogicalPlan);
            return convertToDruidQuery(druidPhysicalPlan);
        }

        @Override
        public String getCategory() {
            return "Druid";
        }

        private RelNode transformToDruidPlan(RelNode root) {

            RelOptPlanner plan = root.getCluster().getPlanner();
            final RelVisitor visitor = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    if (node instanceof TableScan) {
                        final RelOptCluster cluster = node.getCluster();
                        final RelOptTable.ToRelContext context =
                            RelOptUtil.getContext(cluster);
                        final RelNode r = node.getTable().toRel(context);
                        plan.registerClass(r);
                    }
                    super.visit(node, ordinal, parent);
                }
            };

            visitor.go(root);
            Program program = Programs.standard();
            RelTraitSet traits = plan.emptyTraitSet().replace(EnumerableConvention.INSTANCE);

            return program.run(plan, root, traits,
                ImmutableList.<RelOptMaterialization>of(),
                ImmutableList.<RelOptLattice>of());
        }

        private String convertToDruidQuery(RelNode root) {
            return findDruidQuery(root).getQuerySpec().getQueryString(null, 0);
        }

        private DruidQuery findDruidQuery(RelNode relNode) {
            RelNode child = null;
            // when SQL contains union operation,
            // there will be some RelNode with other type between Project and DruidQuery in Logical Tree.
            // Since I cannot find the rule for numbers of the RelNode,
            // loop is used.
            for (child = relNode; !(child instanceof DruidQuery); ) {
                child = child.getInput(0);
            }
            return (DruidQuery) child;
        }

        public String sql() {
            return sql;
        }
    }

    public static class HiveExtractor extends PreparedExtractProcedure {

        public HiveExtractor(QueryProcedure next, Properties properties,
                             FrameworkConfig config, RelNode relNode, String tableName) {
            super(next, properties, config, relNode, tableName);
        }

        @Override
        public String toRecognizedQuery() {
            return sql(new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT));
        }

        @Override
        public String getCategory() {
            return "Hive";
        }
    }

    public static class VirtualExtractor extends PreparedExtractProcedure {

        private String sql;

        public VirtualExtractor(QueryProcedure next, Properties properties,
                                FrameworkConfig config, RelNode relNode,
                                String tableName, String sql) {
            super(next, properties, config, relNode, tableName);
            this.sql = sql;
        }

        @Override
        public String toRecognizedQuery() {
            return sql.replaceAll("\n", " ");
        }

        @Override
        public String getCategory() {
            return "QSQL";
        }
    }

    public static class CsvExtractor extends PreparedExtractProcedure {

        public Properties properties;
        private String sql;

        /**
         * CsvExtractor.
         */
        public CsvExtractor(QueryProcedure next, Properties properties,
                            FrameworkConfig config, RelNode relNode,
                            String tableName, String sql) {
            super(next, properties, config, relNode, tableName);
            this.sql = sql;
            this.properties = properties;
        }

        @Override
        public String toRecognizedQuery() {
            String sql = sql(new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT));
            //TODO Here is one more cost of SQL parsing here. Replace it
            this.properties.put("tableName", SqlUtil.parseTableName(sql)
                .tableNames.get(0).replaceAll("\\.", "_"));
            return sql;
        }

        @Override
        public String getCategory() {
            return "Csv";
        }
    }


    public static class JdbcExtractor extends PreparedExtractProcedure {
        private String dbType;
        private String dialectClassName;
        private String quote;
        private String replaceAll;

        /**
         * JdbcExtractor.
         */
        public JdbcExtractor(QueryProcedure next, Properties properties,
            FrameworkConfig config, RelNode relNode,
            String tableName,String dbType,Map<String,String> paramsMap) {
            super(next, properties, config, relNode, tableName);
            this.dialectClassName = paramsMap.get("dialect");
            this.dbType = dbType;
            this.quote = paramsMap.get("quote");
            this.replaceAll = paramsMap.get("replaceAll");
        }

        @Override
        public String toRecognizedQuery() {
            try {
                Class clz = Class.forName("com.qihoo.qsql.org.apache.calcite.sql.dialect." + dialectClassName);
                Constructor constructor = clz.getConstructor(Context.class);
                Context context = SqlDialect.EMPTY_CONTEXT;
                if (StringUtils.isNotBlank(quote)) {
                    context = context.withIdentifierQuoteString(quote);
                }
                String sql = sql((SqlDialect) constructor.newInstance(context));
                if (StringUtils.isNotBlank(replaceAll)) {
                    String[] split = replaceAll.split(",");
                    sql = sql.replaceAll(split[0], split[1]);
                }
                System.out.println(sql);
                return sql;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return "";
        }

        @Override
        public String getCategory() {
            return dbType;
        }
    }
}
