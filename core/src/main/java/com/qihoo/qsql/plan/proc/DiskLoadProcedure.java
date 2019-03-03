package com.qihoo.qsql.plan.proc;

import com.qihoo.qsql.plan.ProcedureVisitor;

/**
 * Describe the function for saving data into disk.
 */
public class DiskLoadProcedure extends LoadProcedure {

    public String path;
    public boolean isOnCluster = true;

    public DiskLoadProcedure(String path) {
        super(DataFormat.DEFAULT);
        this.path = path;
    }

    public void setResultsOnLocal() {
        this.isOnCluster = false;
    }

    public boolean isResultsOnLocal() {
        return isOnCluster;
    }

    @Override
    public void accept(ProcedureVisitor visitor) {
        visitor.visit(this);
    }
}
