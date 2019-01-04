package com.qihoo.qsql.plan;

public interface Traversable {
    void accept(ProcedureVisitor visitor);
}
