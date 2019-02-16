package com.qihoo.qsql.plan.func;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class SparkSqlDialect extends SqlDialect {

    public SparkSqlDialect(Context context) {
        super(context);
    }

    @Override public boolean supportsAggregateFunction(SqlKind kind) {
        switch (kind) {
            case COUNT:
            case SUM:
            case MIN:
            case MAX:
            case AVG:
            case FIRST:
            case LAST:
                return true;

            default:
                return false;
        }
    }

    @Override public void unparseCall(SqlWriter writer, SqlCall call,
        int leftPrec, int rightPrec) {

        if (call.getOperator() == SqlStdOperatorTable.CONCAT) {
            SqlUtil.unparseFunctionSyntax(SparkSqlOperatorTable.CONCAT, writer, call);
        } else {
            System.out.println();
            //do nothing
        }
    }
}
