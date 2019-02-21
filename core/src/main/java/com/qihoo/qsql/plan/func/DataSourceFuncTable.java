package com.qihoo.qsql.plan.func;

import com.qihoo.qsql.plan.func.SqlRunnerFuncTable.SparkFunctionsHolder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchTable;
import org.apache.calcite.adapter.hive.HiveTable;
import org.apache.calcite.adapter.custom.JdbcTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * .
 */
public class DataSourceFuncTable {

    enum DataSource {
        MYSQL, ELASTICSEARCH, HIVE
    }

    private Map<DataSource, Set<SqlOperator>> sources = new HashMap<>();

    private DataSourceFuncTable() {
        sources.put(DataSource.MYSQL, new MySqlFunctionsHolder().collect());
        sources.put(DataSource.HIVE, new HiveFunctionsHolder().collect());
        sources.put(DataSource.ELASTICSEARCH, new ElasticsearchFunctionsHolder().collect());
    }

    private static DataSourceFuncTable instance = null;

    /**
     * .
     */
    public static DataSourceFuncTable getInstance() {
        if (instance == null) {
            instance = new DataSourceFuncTable();
            return instance;
        }
        return instance;
    }

    interface DataSourceFunctionsHolder {

        Set<SqlOperator> collect();
    }

    static class ElasticsearchFunctionsHolder implements DataSourceFunctionsHolder {

        static final SqlSpecialOperator[] SPECIALS = {
            SqlStdOperatorTable.LIKE, SqlStdOperatorTable.NOT_LIKE
        };

        static final SqlFunction[] FUNCTIONS = {
            //aggregation functions
            SqlStdOperatorTable.AVG,
            SqlStdOperatorTable.COUNT,
            SqlStdOperatorTable.MIN,
            SqlStdOperatorTable.MAX,
            SqlStdOperatorTable.SUM
        };

        @Override
        public Set<SqlOperator> collect() {
            Set<SqlOperator> operators = Arrays.stream(SPECIALS).collect(Collectors.toSet());
            operators.addAll(Arrays.stream(FUNCTIONS).collect(Collectors.toSet()));
            return operators;
        }
    }

    static class MySqlFunctionsHolder implements DataSourceFunctionsHolder {

        static final SqlFunction[] FUNCTIONS = {
            //aggregation functions
            SqlStdOperatorTable.AVG,
            SqlStdOperatorTable.COUNT,
            SqlStdOperatorTable.MIN,
            SqlStdOperatorTable.MAX,
            SqlStdOperatorTable.SUM,

            //date functions
            SqlStdOperatorTable.YEAR,
            SqlStdOperatorTable.MONTH,
            SqlStdOperatorTable.WEEK,
            SqlStdOperatorTable.DAYOFMONTH,
            SqlStdOperatorTable.DAYOFWEEK,
            SqlStdOperatorTable.DAYOFYEAR,
            SqlStdOperatorTable.CURRENT_DATE,
            SqlStdOperatorTable.CURRENT_TIMESTAMP,

            //math functions
            // SqlStdOperatorTable.CEIL -> CEILING(n: Int)
            SqlStdOperatorTable.FLOOR,
            SqlStdOperatorTable.ROUND,
            SqlStdOperatorTable.ABS,

            //string functions
            // SqlStdOperatorTable.LTRIM -> need to add
            // SqlStdOperatorTable.RTRIM -> need to add
            // SqlStdOperatorTable.REVERSE -> need to add
            SqlStdOperatorTable.LENGTH,
            SqlStdOperatorTable.LOWER,
            SqlStdOperatorTable.REPLACE,
            SqlStdOperatorTable.SUBSTRING,
            SqlStdOperatorTable.TRIM,
            SqlStdOperatorTable.UPPER,

            //other functions
            SqlStdOperatorTable.RAND,
            SqlStdOperatorTable.CAST
        };
        static final SqlSpecialOperator[] SPECIALS = {
            SqlStdOperatorTable.LIKE, SqlStdOperatorTable.NOT_LIKE
        };

        @Override
        public Set<SqlOperator> collect() {
            Set<SqlOperator> operators = Arrays.stream(SPECIALS).collect(Collectors.toSet());
            operators.addAll(Arrays.stream(FUNCTIONS).collect(Collectors.toSet()));
            return operators;
        }
    }

    static class HiveFunctionsHolder implements DataSourceFunctionsHolder {

        static final SqlFunction[] FUNCTIONS = SparkFunctionsHolder.FUNCTIONS;

        @Override
        public Set<SqlOperator> collect() {
            return Arrays.stream(FUNCTIONS).collect(Collectors.toSet());
        }
    }

    //TODO aggregate all about data source into a suit of interfaces

    /**
     * .
     */
    public boolean contains(Table table, SqlOperator operator, SqlRunnerFuncTable runnerTable) {
        //TODO remove this condition
        if (! (operator instanceof SqlFunction)) {
            return true;
        }

        if (! runnerTable.contains(operator)) {
            String supportedFunctions = runnerTable.getSupportedFunctions().stream()
                .map(SqlOperator::getName)
                .reduce((x, y) -> x + "," + y)
                .orElse("");
            throw new RuntimeException(String.format(
                "Unsupported function '%s' in runner,"
                    + " Functions supported in current version are \n:%s",
                operator.getName(), supportedFunctions));
        }
        if (table instanceof ElasticsearchTable) {
            return sources.get(DataSource.ELASTICSEARCH).contains(operator);
        } else if (table instanceof JdbcTable) {
            return sources.get(DataSource.MYSQL).contains(operator);
        } else {
            return table instanceof HiveTable && sources.get(DataSource.HIVE).contains(operator);
        }
    }

    //需要统计所支持函数的数据源

    /*
     * MySQL
     * Hive
     * Elasticsearch
     * SparkSQL
     */

    //使用函数的操作(所有使用表达式的操作)

    /*
     * SELECT
     * JOIN     (return BOOLEAN)
     * WHERE    (return BOOLEAN)
     * ORDER BY
     * HAVING   (return BOOLEAN)
     */

    //统一函数的做法
    /* Spark具备的函数
     *      函数名称一致
     *          函数参数一致       -> 跳过
     *          函数参数不一致     -> review函数定义，可向多参数函数填充默认值
     *      函数名称不一致         -> 在数据源方言中添加对指定函数的转义，参考OracleDialect
     * Spark不具备的函数           -> 在生成的Code中向Spark临时注册UDF
     *
     * 数据源不支持的函数           -> 针对函数所处的不同操作对函数及函数对应的操作向Spark上移
     *                              (针对操作进行上移更易实现，拆分后不直接拼接TableScan和Project，
     *                              应根据函数的支持动态拼接相关操作，但需要考虑过滤条件上移会引起性能下降，
     *                              应尽可能根据与或条件拆分表达式进行上下移动)
     *
     * Calcite不具备的函数         -> 在SqlStdOperatorTable中添加相关函数
     *
     * Calcite containsAll Spark -> Spark containsAll DataSource -> DataSource only support themselves
     */

    /*  1. 梳理记录各数据源支持的函数各5个，添加函数校验逻辑
     *  2. 实现针对数据源缺失的函数对应操作的向上移动，以select和where为例
     *  3. 实现方言中对函数的转换
     *  4. 实现Spark自定义函数注册
     */
}
