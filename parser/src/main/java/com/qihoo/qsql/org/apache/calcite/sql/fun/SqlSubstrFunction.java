package com.qihoo.qsql.org.apache.calcite.sql.fun;


import com.google.common.collect.ImmutableList;
import com.qihoo.qsql.org.apache.calcite.linq4j.Ord;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.sql.*;
import com.qihoo.qsql.org.apache.calcite.sql.type.*;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlMonotonicity;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlValidator;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlValidatorScope;

import java.math.BigDecimal;
import java.util.List;

//Created by qsql-team
public class SqlSubstrFunction extends SqlFunction{
    SqlSubstrFunction(){
        super(
            "SUBSTR",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_VARYING,
            null,
            null,
            SqlFunctionCategory.STRING);
    }

    public String getAllowedSignatures(String opName) {
        StringBuilder ret = new StringBuilder();
        for (Ord<SqlTypeName> typeName : Ord.zip(SqlTypeName.STRING_TYPES)) {
            if (typeName.i > 0) {
                ret.append(NL);
            }
            ret.append(
                SqlUtil.getAliasedSignature(this, opName,
                    ImmutableList.of(typeName.e, SqlTypeName.INTEGER)));
            ret.append(NL);
            ret.append(
                SqlUtil.getAliasedSignature(this, opName,
                    ImmutableList.of(typeName.e, SqlTypeName.INTEGER,
                        SqlTypeName.INTEGER)));
        }
        return ret.toString();
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure) {
        SqlValidator validator = callBinding.getValidator();
        SqlValidatorScope scope = callBinding.getScope();

        final List<SqlNode> operands = callBinding.operands();
        int n = operands.size();
        assert (3 == n) || (2 == n);
        if (!OperandTypes.STRING.checkSingleOperandType(
            callBinding,
            operands.get(0),
            0,
            throwOnFailure)) {
            return false;
        }
        if (2 == n) {
            if (!OperandTypes.NUMERIC.checkSingleOperandType(
                callBinding,
                operands.get(1),
                0,
                throwOnFailure)) {
                return false;
            }
        } else {
            RelDataType t1 = validator.deriveType(scope, operands.get(1));
            RelDataType t2 = validator.deriveType(scope, operands.get(2));

            if (SqlTypeUtil.inCharFamily(t1)) {
                if (!OperandTypes.STRING.checkSingleOperandType(
                    callBinding,
                    operands.get(1),
                    0,
                    throwOnFailure)) {
                    return false;
                }
                if (!OperandTypes.STRING.checkSingleOperandType(
                    callBinding,
                    operands.get(2),
                    0,
                    throwOnFailure)) {
                    return false;
                }

                if (!SqlTypeUtil.isCharTypeComparable(callBinding, operands,
                    throwOnFailure)) {
                    return false;
                }
            } else {
                if (!OperandTypes.NUMERIC.checkSingleOperandType(
                    callBinding,
                    operands.get(1),
                    0,
                    throwOnFailure)) {
                    return false;
                }
                if (!OperandTypes.NUMERIC.checkSingleOperandType(
                    callBinding,
                    operands.get(2),
                    0,
                    throwOnFailure)) {
                    return false;
                }
            }

            if (!SqlTypeUtil.inSameFamily(t1, t2)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        }
        return true;
    }

    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        // SUBSTRING(x FROM 0 FOR constant) has same monotonicity as x
        if (call.getOperandCount() == 3) {
            final SqlMonotonicity mono0 = call.getOperandMonotonicity(0);
            if ((mono0 != SqlMonotonicity.NOT_MONOTONIC)
                && call.getOperandMonotonicity(1) == SqlMonotonicity.CONSTANT
                && call.getOperandLiteralValue(1, BigDecimal.class)
                .equals(BigDecimal.ZERO)
                && call.getOperandMonotonicity(2) == SqlMonotonicity.CONSTANT) {
                return mono0.unstrict();
            }
        }
        return super.getMonotonicity(call);
    }


}
