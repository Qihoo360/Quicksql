package com.qihoo.qsql.plan.proc;

import com.qihoo.qsql.plan.ProcedureVisitor;
import java.util.List;

/**
 * Decide the display format of result.
 */
public class LoadProcedure extends QueryProcedure {

    private DataFormat format;

    public LoadProcedure(DataFormat format) {
        super(null);
        this.format = format;
    }

    public DataFormat getDataFormat() {
        return format;
    }

    @Override
    public int getValue() {
        return 0x00;
    }

    @Override
    public StringBuilder digest(StringBuilder builder, List<String> tabs) {
        String prefix = tabs.stream().reduce((x, y) -> x + y).orElse("");
        StringBuilder newBuilder = builder.append(prefix).append("[LoadProcedure]")
            .append("\n").append(prefix)
            .append(" \"data_format\":").append(format);

        if (next() != null) {
            return next().digest(newBuilder, tabs);
        } else {
            return newBuilder;
        }
    }

    @Override
    public void accept(ProcedureVisitor visitor) {
        visitor.visit(this);
    }

    public enum DataFormat {
        CSV, JSON, PARQUET, DEFAULT
    }
}
