package com.qihoo.qsql.plan.proc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.qihoo.qsql.metadata.MetadataMapping;
import com.qihoo.qsql.utils.SqlUtil;
import java.io.IOException;
import java.util.AbstractList;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.adapter.csv.CsvTable;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchRel;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchRules;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchTable;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchTranslatableTable;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.hive.HiveTable;
import org.apache.calcite.adapter.custom.JdbcTable;
import org.apache.calcite.adapter.virtual.VirtualTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

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
        SqlNode sqlNode) {
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
        } else if (relOptTable.getTable() instanceof JdbcTable) {
            //TODO add more jdbc type
            String dbType = ((JdbcTable) relOptTable.getTable())
                .getProperties().getProperty("dbType", "unknown");
            switch (dbType) {
                case MetadataMapping.MYSQL:
                    return new MySqlExtractor(next, ((JdbcTable) relOptTable.getTable())
                        .getProperties(), config, relNode, tableName);
                case MetadataMapping.ORACLE:
                    return new OracleExtractor(next, ((JdbcTable) relOptTable.getTable())
                        .getProperties(), config, relNode, tableName);
                default:
                    throw new RuntimeException("");
            }

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
            properties.put("esQuery", esJson.replaceAll(" ", ""));
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
            for (child = relNode; ! (child instanceof DruidQuery); ) {
                child = child.getInput(0);
            }
            return (DruidQuery) child;
        }

        public String sql() {
            return sql;
        }
    }

    public static class MySqlExtractor extends PreparedExtractProcedure {

        public MySqlExtractor(QueryProcedure next, Properties properties,
            FrameworkConfig config, RelNode relNode,
            String tableName) {
            super(next, properties, config, relNode, tableName);
        }

        @Override
        public String toRecognizedQuery() {
            return sql(new MysqlSqlDialect(SqlDialect.EMPTY_CONTEXT)).toLowerCase();
        }

        @Override
        public String getCategory() {
            return "MySQL";
        }
    }

    public static class OracleExtractor extends PreparedExtractProcedure {

        public OracleExtractor(QueryProcedure next, Properties properties,
            FrameworkConfig config, RelNode relNode,
            String tableName) {
            super(next, properties, config, relNode, tableName);
        }

        @Override
        public String toRecognizedQuery() {
            return sql(new OracleSqlDialect(SqlDialect.EMPTY_CONTEXT));
        }

        @Override
        public String getCategory() {
            return "Oracle";
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
}
