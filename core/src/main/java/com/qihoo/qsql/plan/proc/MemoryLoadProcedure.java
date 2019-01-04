package com.qihoo.qsql.plan.proc;

/**
 * Describe the function for saving data into memory.
 */
public class MemoryLoadProcedure extends LoadProcedure {

    public MemoryLoadProcedure() {
        super(DataFormat.DEFAULT);
    }
}
