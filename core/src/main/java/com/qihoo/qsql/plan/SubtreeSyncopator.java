package com.qihoo.qsql.plan;

import com.google.common.collect.ImmutableList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchTable;
import org.apache.calcite.adapter.virtual.VirtualTable;
import org.apache.calcite.adapter.virtual.VirtualTypeSystem;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Travel the whole {@link RelNode} of SQL line, cut it based on a algorithm and return the final sub-trees and their
 * alias names, which will be used to create {@link com.qihoo.qsql.plan.proc.QueryProcedure} chain. <p> The algorithm
 * trying to sink more calculation into the data source as more as possible, which can reduce the data transforming and
 * pressure of IO. For more, it also can reduce the resource need for calculation engine. </p>
 */
public class SubtreeSyncopator extends RelShuttleImpl {

    // Table name separator
    private static final String THE_SAME_TABLE_SEPARATOR = "_";
    Map<RelNode, AbstractMap.SimpleEntry<String, RelOptTable>> rootNodeSchemas = new LinkedHashMap<>();
    private RelNode relNode;
    // Incrementing alias index
    private Integer aliasIndex = 0;

    private SubtreeSyncopator() {
    }

    SubtreeSyncopator(RelNode origin) {
        this.relNode = origin;
        pruneTree(origin);
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        return completeLogicalTree(join);
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return completeLogicalTree(union);
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return completeLogicalTree(intersect);
    }

    @Override
    public RelNode visit(LogicalProject project) {
        List<RexNode> rexNodes = project.getChildExps();
        RexNodeSyncopator syncopator = new RexNodeSyncopator();

        for (RexNode node : rexNodes) {
            Map<RelNode, AbstractMap.SimpleEntry<String, RelOptTable>> map =
                node.accept(syncopator);
            if (map != null) {
                rootNodeSchemas.putAll(map);
            }
        }

        return project.getInput().accept(this);
    }

    @Override
    public RelNode visit(TableScan scan) {
        return scan;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        return aggregate.getInput().accept(this);
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        return match.getInput().accept(this);
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        return filter.getInput().accept(this);
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        return sort.getInput().accept(this);
    }

    public RelNode visit(DruidQuery druidQuery) {
        return visit(druidQuery.getTableScan());
    }

    public RelNode visit(LogicalValues values) {
        return values;
    }

    private RelNode completeLogicalTree(RelNode binaryNode) {
        RelNode left = binaryNode.getInputs().get(0).accept(this);
        RelNode right = binaryNode.getInputs().get(1).accept(this);

        if (! isValues(left) && ! isValues(right)) {
            return neededToSyncopate(binaryNode, transferTableScan(left), transferTableScan(right))
                ? new MixedTableScan(transferTableScan(left)) : left;
        }
        //return value is just an identification to parent node
        // so "left" or "right" has no difference
        return left;
    }

    private boolean neededToSyncopate(RelNode parent,
        TableScan left,
        TableScan right) {
        if (left instanceof MixedTableScan && right instanceof MixedTableScan) {
            return false;
        }

        if (left instanceof MixedTableScan) {
            pruneSubtree(parent, right, 1);
            return true;
        }

        if (right instanceof MixedTableScan) {
            pruneSubtree(parent, left, 0);
            return true;
        }

        if (shouldBeDivided(left.getTable(), right.getTable())) {
            pruneSubtree(parent, left, 0);
            pruneSubtree(parent, right, 1);
            return true;
        }

        return false;
    }

    private boolean shouldBeDivided(RelOptTable left, RelOptTable right) {
        RelOptTableImpl leftImpl = ((RelOptTableImpl) left);
        RelOptTableImpl rightImpl = ((RelOptTableImpl) right);

        Table leftTable = leftImpl.getTable();
        Table rightTable = rightImpl.getTable();

        return notSupportedBinOp(leftTable, leftTable)
            || isDiffFromEachOther(leftTable, rightTable);
    }

    private boolean isDiffFromEachOther(Table left, Table right) {
        if (! isDiffClassFromEachOther(left, right)) {
            return ! (left instanceof TranslatableTable)
                || ! (right instanceof TranslatableTable)
                || isDiffDbFromEachOther((TranslatableTable) left,
                (TranslatableTable) right);
        }
        return true;
    }

    private boolean isDiffClassFromEachOther(Table left, Table right) {
        return ! left.getClass().equals(right.getClass());
    }

    private boolean isDiffDbFromEachOther(TranslatableTable left, TranslatableTable right) {
        return ! left.getBaseName().toLowerCase().equals(right.getBaseName().toLowerCase());
    }

    private boolean notSupportedBinOp(Table left, Table right) {
        return ((left instanceof ElasticsearchTable)
            && (right instanceof ElasticsearchTable)
            || (left instanceof DruidQuery
            && right instanceof DruidQuery));
    }

    private void executePruningSubtree(RelNode secondLevelNode,
        RelOptTable table,
        String tempTableName) {
        LogicalProject newProject = LogicalProject.create(
            secondLevelNode,
            secondLevelNode
                .getCluster()
                .getRexBuilder()
                .identityProjects(secondLevelNode.getRowType()),
            secondLevelNode
                .getRowType()
                .getFieldNames());

        rootNodeSchemas.put(newProject, new AbstractMap.SimpleEntry<>(tempTableName, table));
    }

    private void pruneTree(RelNode origin) {
        //first loop, multiple engine sql will be pruned in this loop
        RelNode rel = origin.accept(this);
        //if final rel is tableScan, then transfer tableScan to TemporaryTableScan
        //then save to rootNodeSchemas
        handleFinalRelNode(rel);
    }

    private Boolean isDruidQuery(RelNode relNode) {
        return relNode instanceof DruidQuery;
    }

    private Boolean isValues(RelNode relNode) {
        return relNode instanceof Values;
    }

    private Boolean isSingleTableScan(RelNode relNode) {
        return (relNode instanceof TableScan && ! ((TableScan) relNode).getClass().equals(MixedTableScan.class))
            || isDruidQuery(relNode);
    }

    private TableScan transferTableScan(RelNode relNode) {
        if (isDruidQuery(relNode)) {
            return ((DruidQuery) relNode).getTableScan();
        } else {
            return (TableScan) relNode;
        }
    }

    private void handleFinalRelNode(RelNode relNode) {

        if (isSingleTableScan(relNode)) {
            TableScan tableScan = transferTableScan(relNode);
            handleFinalTableScan(tableScan);
        } else if (isValues(relNode)) {
            handleFinalValues();
        }

    }

    private void handleFinalTableScan(TableScan tableScan) {
        RelOptTableImpl relOptTable = ((RelOptTableImpl) tableScan.getTable());
        List<String> tableAlias = createTableAlias(relOptTable.getNames());
        rootNodeSchemas.put(this.relNode,
            new AbstractMap.SimpleEntry<>(tableAlias.get(0), tableScan.getTable()));
    }

    private void handleFinalValues() {

        if (rootNodeSchemas.size() > 0) {
            throw new RuntimeException("Prune SubTree Error");
        } else {
            List<String> tableAlias = createTableAlias(null);
            rootNodeSchemas.put(this.relNode,
                new AbstractMap.SimpleEntry<>(tableAlias.get(0), createVirtualRelOptTable()));
        }
    }

    private RelOptTable createVirtualRelOptTable() {

        List<String> names = createVirtualTableName();
        RelDataTypeSystem relDataTypeSystem = new VirtualTypeSystem();
        RelDataTypeFieldImpl relDataTypeField = new RelDataTypeFieldImpl(
            "columnName", 0, new BasicSqlType(relDataTypeSystem, SqlTypeName.ANY));
        List<RelDataTypeField> relDataTypeFieldList = new ArrayList<>();
        relDataTypeFieldList.add(relDataTypeField);

        return RelOptTableImpl.create(null,
            new RelRecordType(relDataTypeFieldList),
            new VirtualTable(names.get(0)),
            names,
            null);
    }

    private List<String> createVirtualTableName() {
        List<String> names = new ArrayList<>();
        //dbName, UseLess
        names.add("TEST");
        //tableName, UseLess
        names.add("TEST" + this.aliasIndex);
        this.aliasIndex++;
        return names;
    }

    private void pruneSubtree(RelNode parent,
        TableScan scan,
        int leftOrRight) {

        RelOptTableImpl lowLevelTable = ((RelOptTableImpl) scan.getTable());

        List<String> newNames = createTableAlias(lowLevelTable.getNames());

        RelNode originChild = parent.getInputs().get(leftOrRight);
        executePruningSubtree(originChild, scan.getTable(), newNames.get(0));

        TableScan tempTableScan = createTemporaryTableScan(lowLevelTable, newNames, scan,
            originChild);

        parent.replaceInput(leftOrRight, tempTableScan);
    }

    private List<String> createTableAlias(List<String> originalNames) {
        List<String> newNames = new ArrayList<>();
        if (originalNames == null) {
            newNames.add("tempTable" + THE_SAME_TABLE_SEPARATOR + aliasIndex);
        } else {
            newNames.add(originalNames.get(0) + THE_SAME_TABLE_SEPARATOR
                + originalNames.get(1) + THE_SAME_TABLE_SEPARATOR + aliasIndex);
        }
        aliasIndex++;
        return newNames;
    }

    private TableScan createTemporaryTableScan(RelOptTableImpl relOptTable,
        List<String> tableAlias,
        TableScan tableScan,
        RelNode originTable) {
        RelDataType type = originTable.getRowType();

        RelOptTableImpl relOptTableWithAlias = relOptTable.modifyTableName(tableAlias);

        RelOptTable newRelOptTable = RelOptTableImpl.create(
            relOptTableWithAlias.getRelOptSchema(),
            type,
            relOptTableWithAlias.getTable(),
            ImmutableList.copyOf(relOptTableWithAlias.getQualifiedName())
        );

        return new TemporaryTableScan(
            tableScan.getCluster(),
            tableScan.getTraitSet(),
            newRelOptTable
        );
    }

    /**
     * Special tableScan type for non-join RelNode, eg. LogicalFilter, LogicalProject. where there is a
     * TemporaryTableScan, there has been cut
     */
    private class TemporaryTableScan extends TableScan {

        TemporaryTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
            super(cluster, traitSet, table);
        }
    }

    /**
     * Special tableScan type for join RelNode, eg. LogicalJoin, LogicalUnion. where there is a TemporaryTableScan,
     * there has been cut
     */
    private class MixedTableScan extends TableScan {

        MixedTableScan(TableScan scan) {
            super(scan.getCluster(), scan.getTraitSet(), scan.getTable());
        }
    }

    class RexNodeSyncopator extends
        RexVisitorImpl<Map<RelNode, AbstractMap.SimpleEntry<String, RelOptTable>>> {

        private SubtreeSyncopator syncopator = new SubtreeSyncopator();

        RexNodeSyncopator() {
            super(true);
        }

        public Map<RelNode, AbstractMap.SimpleEntry<String, RelOptTable>> visitSubQuery(
            RexSubQuery subQuery) {
            subQuery.rel.accept(syncopator);

            for (RexNode operand : subQuery.operands) {
                operand.accept(this);
            }

            return syncopator.rootNodeSchemas;
        }

    }
}
