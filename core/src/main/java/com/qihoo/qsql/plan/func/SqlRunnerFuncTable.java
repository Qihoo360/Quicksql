package com.qihoo.qsql.plan.func;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import java.util.Arrays;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable.Key;

/**
 * .
 */
public class SqlRunnerFuncTable {
    private final Multimap<Key, SqlOperator> operators = HashMultimap.create();

    private static SqlRunnerFuncTable INSTANCE = null;
    private RunnerType runner = null;

    private SqlRunnerFuncTable(RunnerType runner) {
        RunnerFunctionsHolder holder;
        this.runner = runner;
        switch (runner) {
            case SPARK:
                holder = new SparkFunctionsHolder();
                break;
            case JDBC:
            case DEFAULT:
                holder = new JdbcFunctionHolder();
                break;
            default:
                throw new RuntimeException("Unsupported runner.");
        }
        holder.registerAll(operators);
    }

    /**
     * .
     */
    public static SqlRunnerFuncTable getInstance(RunnerType runner) {
        if (INSTANCE == null || INSTANCE.runner != runner) {
            INSTANCE = new SqlRunnerFuncTable(runner);
            return INSTANCE;
        }
        return INSTANCE;
    }

    interface RunnerFunctionsHolder {
        void registerAll(Multimap<Key, SqlOperator> operators);
    }

    static class SparkFunctionsHolder implements RunnerFunctionsHolder {
        //TODO change to remove strategy
        static final SqlFunction[] FUNCTIONS = {
            //aggregation functions
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            SqlStdOperatorTable.AVG,
            SqlStdOperatorTable.COUNT,
            SqlStdOperatorTable.FIRST,
            SqlStdOperatorTable.LAST,
            SqlStdOperatorTable.MIN,
            SqlStdOperatorTable.MAX,
            SqlStdOperatorTable.SUM,
            //collection functions

            //date functions
            SqlStdOperatorTable.YEAR,
            SqlStdOperatorTable.MONTH,
            SqlStdOperatorTable.WEEK,
            SqlStdOperatorTable.DAYOFMONTH,
            SqlStdOperatorTable.DAYOFWEEK,
            SqlStdOperatorTable.DAYOFYEAR,
            SqlStdOperatorTable.CURRENT_DATE,
            SqlStdOperatorTable.CURRENT_TIMESTAMP,
            // SqlStdOperatorTable.DATETIME_PLUS -> date_add(start: Column, days: Int)
            // SqlStdOperatorTable.TIMESTAMP_DIFF -> datediff(end: Column, start: column)

            //math functions
            SqlStdOperatorTable.CEIL,
            SqlStdOperatorTable.FLOOR,
            SqlStdOperatorTable.ROUND,
            SqlStdOperatorTable.ABS,

            //misc functions
            // SqlStdOperatorTable.MD5   -> need to add
            // SqlStdOperatorTable.HASH  -> need to add

            //string functions
            // SqlStdOperatorTable.BASE64 -> need to add
            // SqlStdOperatorTable.UNBASE64 -> need to add
            // SqlStdOperatorTable.CONCAT -> concat(expr: Column*)

            SqlStdOperatorTable.LTRIM,
            SqlStdOperatorTable.RTRIM,
            SqlStdOperatorTable.REVERSE,
            SqlStdOperatorTable.LENGTH,
            SqlStdOperatorTable.REGEXP_EXTRACT,
            SqlStdOperatorTable.REGEXP_REPLACE,
            SqlStdOperatorTable.IF,
            // SqlStdOperatorTable.IFNULL -> need to add
            // SqlStdOperatorTable.SPLIT -> need to add
            SqlStdOperatorTable.LOWER,
            SqlStdOperatorTable.REPLACE,
            SqlStdOperatorTable.SUBSTRING,
            SqlStdOperatorTable.TRIM,
            SqlStdOperatorTable.UPPER,
            
            //other functions
            SqlStdOperatorTable.RAND,
            SqlStdOperatorTable.CAST

            //window functions
        };

        // *** SqlStdOperatorTable.COUNT_DISTINCT -> count(distinct)
        // *** SqlStdOperatorTable.IF -> IF(column is null, C.type, null)
        // *** SqlStdOperatorTable. -> IF(column is null, C.type, null)

        static final SqlSpecialOperator[] SPECIALS = {
            SqlStdOperatorTable.LIKE,
            SqlStdOperatorTable.NOT_LIKE
        };

        @Override
        public void registerAll(Multimap<Key, SqlOperator> operators) {
            Arrays.stream(FUNCTIONS).forEach(func ->
                operators.put(new Key(func.getName(), SqlSyntax.FUNCTION), func));
            Arrays.stream(SPECIALS).forEach(spec ->
                operators.put(new Key(spec.getName(), SqlSyntax.SPECIAL), spec));
        }
    }

    static class JdbcFunctionHolder implements RunnerFunctionsHolder {
        //contains all of Calcite operators
        @Override
        public void registerAll(Multimap<Key, SqlOperator> operators) {
            SqlStdOperatorTable.instance()
                .getOperators().entries()
                .forEach(op -> operators.put(op.getKey(), op.getValue()));
        }
    }

    public boolean contains(SqlOperator operator) {
        return operators.containsKey(
            new Key(operator.getName(), SqlSyntax.FUNCTION));
    }

    public RunnerType getRunner() {
        return runner;
    }
}
