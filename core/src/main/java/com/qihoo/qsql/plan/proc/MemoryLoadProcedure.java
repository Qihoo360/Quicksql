package com.qihoo.qsql.plan.proc;

import com.qihoo.qsql.plan.ProcedureVisitor;

/**
 * Describe the function for saving data into memory.
 */
public class MemoryLoadProcedure extends LoadProcedure {

    public MemoryLoadProcedure() {
        super(DataFormat.DEFAULT);
    }

    @Override
    public void accept(ProcedureVisitor visitor) {
        visitor.visit(this);
    }
}
