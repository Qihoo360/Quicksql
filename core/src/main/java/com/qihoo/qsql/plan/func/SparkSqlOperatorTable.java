package com.qihoo.qsql.plan.func;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class SparkSqlOperatorTable extends ReflectiveSqlOperatorTable {

    public static final SqlFunction CONCAT =
        new SqlFunction("CONCAT", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_VARYING, null,
            OperandTypes.STRING_SAME_SAME,
            SqlFunctionCategory.STRING);
}
