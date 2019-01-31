package org.apache.calcite.sql.fun;

import java.util.List;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlMonotonicity;

//Updated by qsql-team
public class SqlRegexpReplaceFunction extends SqlFunction {
    public SqlRegexpReplaceFunction() {
        super(
            "REGEXP_REPLACE",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_VARYING,
            null,
            null,
            SqlFunctionCategory.STRING);
    }

    @Override
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure
    ) {
        final List<SqlNode> operands = callBinding.operands();
        int n = operands.size();
        assert (3 == n);

        return OperandTypes.STRING
            .checkSingleOperandType(callBinding, operands.get(0), 0, throwOnFailure)
            && OperandTypes.STRING
            .checkSingleOperandType(callBinding, operands.get(1), 0, throwOnFailure)
            && OperandTypes.STRING
            .checkSingleOperandType(callBinding, operands.get(2), 0, throwOnFailure);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(3, 3);
    }

    @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return SqlMonotonicity.INCREASING;
    }
}
