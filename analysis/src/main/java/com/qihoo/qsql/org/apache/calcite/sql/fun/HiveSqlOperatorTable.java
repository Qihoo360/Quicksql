package com.qihoo.qsql.org.apache.calcite.sql.fun;

import com.qihoo.qsql.org.apache.calcite.sql.SqlFunction;
import com.qihoo.qsql.org.apache.calcite.sql.SqlFunctionCategory;
import com.qihoo.qsql.org.apache.calcite.sql.SqlKind;
import com.qihoo.qsql.org.apache.calcite.sql.type.OperandTypes;
import com.qihoo.qsql.org.apache.calcite.sql.type.ReturnTypes;
import com.qihoo.qsql.org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class HiveSqlOperatorTable extends ReflectiveSqlOperatorTable {
    public static final SqlFunction CONCAT =
        new SqlFunction("CONCAT", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_VARYING, null,
            OperandTypes.STRING_SAME_SAME,
            SqlFunctionCategory.STRING);
}
