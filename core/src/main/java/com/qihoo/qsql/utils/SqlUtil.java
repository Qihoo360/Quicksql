package com.qihoo.qsql.utils;

import com.qihoo.qsql.exec.JdbcPipeline;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.plan.QueryTables;
import com.qihoo.qsql.plan.TableNameCollector;
import java.util.List;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParseException;

/**
 * Sql related utils.
 */
public class SqlUtil {
    /**
     * Parse table names.
     *
     * @param sql sql string
     * @return table names
     */
    //TODO reconstruct `QueryTables` to fit the data source directly to HDFS
    public static QueryTables parseTableName(String sql) {
        TableNameCollector collector = new TableNameCollector();
        try {
            return collector.parseTableName(sql);
        } catch (SqlParseException ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    /**
     * Get metadata.
     *
     * @param tableNames table names
     * @return metadata
     */
    public static String getSchemaPath(List<String> tableNames) {
        String path = "inline: " + MetadataPostman.getCalciteModelSchema(tableNames);
        if (path.equals("inline: ")) {
            path = JdbcPipeline.CSV_DEFAULT_SCHEMA;
        }
        return path;
    }
}
