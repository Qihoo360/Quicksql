package com.qihoo.qsql.plan.func;

import com.qihoo.qsql.org.apache.calcite.config.NullCollation;
import com.qihoo.qsql.org.apache.calcite.sql.SqlCall;
import com.qihoo.qsql.org.apache.calcite.sql.SqlDialect;
import com.qihoo.qsql.org.apache.calcite.sql.SqlKind;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.SqlUtil;
import com.qihoo.qsql.org.apache.calcite.sql.SqlWriter;
import com.qihoo.qsql.org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class SparkSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT =
        new SparkSqlDialect(EMPTY_CONTEXT
            .withDatabaseProduct(DatabaseProduct.HIVE)
            .withNullCollation(NullCollation.LOW));

    public SparkSqlDialect(Context context) {
        super(context);
    }

    @Override
    public boolean supportsAggregateFunction(SqlKind kind) {
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

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call,
        int leftPrec, int rightPrec) {
        if (call.getOperator() == SqlStdOperatorTable.CONCAT) {
            SqlUtil.unparseFunctionSyntax(SparkSqlOperatorTable.CONCAT, writer, call);
        } else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }

    @Override
    public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
        SqlNode fetch) {
        unparseFetchUsingLimit(writer, offset, fetch);
    }

    @Override
    public SqlNode emulateNullDirection(SqlNode node,
        boolean nullsFirst, boolean desc) {
        return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
    }

    @Override
    public boolean supportsCharSet() {
        return false;
    }
}
