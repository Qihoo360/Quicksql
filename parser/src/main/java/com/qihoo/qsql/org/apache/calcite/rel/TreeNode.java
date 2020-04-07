package com.qihoo.qsql.org.apache.calcite.rel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * condition/tableName/dbName/function/universe
 */

public class TreeNode {
    private List<TreeNode> child;
    private String name;
    private String universe;
    private List<String> keySet;
    private Map<String, String> commonMap;

    public TreeNode() {
        child = new ArrayList<>();
    }

    public List<TreeNode> getChild() {
        return child;
    }

    public void setChild(List<TreeNode> child) {
        this.child = child;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUniverse() {
        return universe;
    }

    public void setUniverse(String universe) {
        this.universe = universe;
    }

    public List<String> getKeySet() {
        return keySet;
    }

    public void setKeySet(List<String> keySet) {
        this.keySet = keySet;
    }

    public Map<String, String> getCommonMap() {
        return commonMap;
    }

    public void setCommonMap(Map<String, String> commonMap) {
        this.commonMap = commonMap;
    }


}
