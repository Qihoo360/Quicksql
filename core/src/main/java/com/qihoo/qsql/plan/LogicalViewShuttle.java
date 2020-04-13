package com.qihoo.qsql.plan;

import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.RelViewShuttle;
import com.qihoo.qsql.org.apache.calcite.rel.TreeNode;
import com.qihoo.qsql.org.apache.calcite.rel.core.TableFunctionScan;
import com.qihoo.qsql.org.apache.calcite.rel.core.TableScan;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalAggregate;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalCorrelate;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalExchange;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalFilter;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalIntersect;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalJoin;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalMatch;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalMinus;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalProject;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalSort;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalUnion;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalValues;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;


public class LogicalViewShuttle implements RelViewShuttle {


    @Override
    public TreeNode visit(LogicalAggregate aggregate) {
        TreeNode child = aggregate.getInput().accept(this);
        TreeNode result = new TreeNode();
        List<TreeNode> childList = new ArrayList<>();
        childList.add(child);
        result.setChild(childList);
        List<String> list = new ArrayList<>();
        list.add(aggregate.getGroupType().toString());
        Map<String, String> map = new HashMap<>();
        map.put(aggregate.getRelTypeName(), aggregate.getChildExps().toString());
        result.setCommonMap(map);
        result.setName(aggregate.getRelTypeName());
        result.setUniverse(aggregate.groupSets.toString());
        result.setKeySet(list);
        return result;
    }

    @Override
    public TreeNode visit(LogicalMatch match) {
        TreeNode child = match.getInput().accept(this);
        TreeNode result = new TreeNode();
        List<String> list = new ArrayList<>();
        List<TreeNode> childList = new ArrayList<>();
        childList.add(child);
        result.setChild(childList);
        list.add(match.getRelTypeName());
        Map<String, String> map = new HashMap<>();
        map.put(match.getRelTypeName(), match.getMeasures().toString());
        result.setName(match.getRelTypeName());
        result.setCommonMap(map);
        result.setKeySet(list);
        return result;
    }

    @Override
    public TreeNode visit(TableScan scan) {
        TreeNode child = null;
        TreeNode result = new TreeNode();
        List<TreeNode> childList = new ArrayList<>();
        if (scan.getInputs().size() > 0) {

            for (RelNode relNode : scan.getInputs()) {
                child = relNode.accept(this);
                childList.add(child);
            }
        }
        result.setChild(childList);
        Map<String, String> map = new HashMap<>();
        map.put(scan.getRelTypeName(), StringUtils.join(scan.getTable().getQualifiedName(), "."));
        List<String> list = new ArrayList<>();
        list.add(scan.getRelTypeName());
        result.setKeySet(list);
        result.setName(scan.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

    @Override
    public TreeNode visit(TableFunctionScan scan) {
        TreeNode result = new TreeNode();
        Map<String, String> map = new HashMap<>();
        map.put(scan.getRelTypeName(), StringUtils.join(scan.getTable().getQualifiedName(), "."));
        List<String> list = new ArrayList<>();
        list.add(scan.getRelTypeName());
        result.setKeySet(list);
        result.setName(scan.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

    @Override
    public TreeNode visit(LogicalValues values) {
        TreeNode result = new TreeNode();
        Map<String, String> map = new HashMap<>();
        map.put(values.getRelTypeName(), values.getChildExps().toString());
        List<String> list = new ArrayList<>();
        list.add(values.getRelTypeName());
        result.setKeySet(list);
        result.setName(values.getRelTypeName());
        result.setCommonMap(map);
        return null;
    }

    @Override
    public TreeNode visit(LogicalFilter filter) {
        TreeNode child = filter.getInput().accept(this);
        TreeNode result = new TreeNode();
        Map<String, String> map = new HashMap<>();
        List<TreeNode> childList = new ArrayList<>();
        childList.add(child);
        result.setChild(childList);
        map.put(filter.getRelTypeName(), filter.getCondition().toString());
        List<String> list = new ArrayList<>();
        list.add(filter.getRelTypeName());
        result.setKeySet(list);
        result.setName(filter.getRelTypeName());
        result.setCommonMap(map);

        return result;
    }

    @Override
    public TreeNode visit(LogicalProject project) {
        TreeNode child = project.getInput().accept(this);
        TreeNode result = new TreeNode();
        List<TreeNode> childList = new ArrayList<>();
        childList.add(child);
        result.setChild(childList);
        Map<String, String> map = new HashMap<>();
        map.put(project.getRelTypeName(), project.getInputs().toString());
        List<String> list = new ArrayList<>();
        list.add(project.getRelTypeName());
        result.setKeySet(list);
        result.setName(project.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

    @Override
    public TreeNode visit(LogicalJoin join) {
        TreeNode rightNode = join.getRight().accept(this);
        TreeNode leftNode = join.getLeft().accept(this);
        TreeNode result = new TreeNode();
        List<TreeNode> childList = new ArrayList<>();
        childList.add(rightNode);
        childList.add(leftNode);
        result.setChild(childList);
        Map<String, String> map = new HashMap<>();
        map.put(join.getJoinType().toString(), join.getRight().toString() + join
            .getRight().toString());
        List<String> list = new ArrayList<>();
        list.add(join.getJoinType().toString());
        result.setKeySet(list);
        result.setName(join.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

    @Override
    public TreeNode visit(LogicalCorrelate correlate) {
        TreeNode result = new TreeNode();
        Map<String, String> map = new HashMap<>();
        map.put(correlate.getRelTypeName(), correlate.getChildExps().toString());
        List<String> list = new ArrayList<>();
        list.add(correlate.getRelTypeName());
        result.setKeySet(list);
        result.setName(correlate.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

    @Override
    public TreeNode visit(LogicalUnion union) {
        TreeNode child = null;
        TreeNode result = new TreeNode();
        Map<String, String> map = new HashMap<>();
        List<TreeNode> childList = new ArrayList<>();
        if (union.getInputs().size() > 0) {
            for (RelNode relNode : union.getInputs()) {
                child = relNode.accept(this);
                childList.add(child);
            }
        }
        result.setChild(childList);
        map.put(union.getRelTypeName(), union.getInputs().toString());
        List<String> list = new ArrayList<>();
        list.add(union.getRelTypeName());
        result.setKeySet(list);
        result.setName(union.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

    @Override
    public TreeNode visit(LogicalIntersect intersect) {
        TreeNode result = new TreeNode();
        Map<String, String> map = new HashMap<>();
        map.put(intersect.getRelTypeName(), intersect.getChildExps().toString());
        List<String> list = new ArrayList<>();
        list.add(intersect.getRelTypeName());
        result.setKeySet(list);
        result.setName(intersect.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

    @Override
    public TreeNode visit(LogicalMinus minus) {
        TreeNode result = new TreeNode();
        Map<String, String> map = new HashMap<>();
        map.put(minus.getRelTypeName(), minus.getChildExps().toString());
        List<String> list = new ArrayList<>();
        list.add(minus.getRelTypeName());
        result.setKeySet(list);
        result.setName(minus.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

    @Override
    public TreeNode visit(LogicalSort sort) {
        TreeNode child = sort.getInput().accept(this);
        TreeNode result = new TreeNode();
        List<TreeNode> childList = new ArrayList<>();
        childList.add(child);
        result.setChild(childList);
        Map<String, String> map = new HashMap<>();
        map.put(sort.getRelTypeName(), sort.getInputs().toString());
        List<String> list = new ArrayList<>();
        list.add(sort.getRelTypeName());
        result.setKeySet(list);
        result.setName(sort.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

    @Override
    public TreeNode visit(LogicalExchange exchange) {
        TreeNode child = exchange.getInput().accept(this);
        TreeNode result = new TreeNode();
        List<TreeNode> childList = new ArrayList<>();
        childList.add(child);
        result.setChild(childList);
        Map<String, String> map = new HashMap<>();
        map.put(exchange.getRelTypeName(), exchange.getInputs().toString());
        List<String> list = new ArrayList<>();
        list.add(exchange.getRelTypeName());
        result.setKeySet(list);
        result.setName(exchange.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

    @Override
    public TreeNode visit(RelNode other) {
        TreeNode result = new TreeNode();
        Map<String, String> map = new HashMap<>();
        map.put(other.getRelTypeName(), other.getInputs().toString());
        List<String> list = new ArrayList<>();
        list.add(other.getRelTypeName());
        result.setKeySet(list);
        result.setName(other.getRelTypeName());
        result.setCommonMap(map);
        return result;
    }

}
