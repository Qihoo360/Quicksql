package com.qihoo.qsql.utils;

import com.qihoo.qsql.exec.JdbcPipeline;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.plan.TableNameCollector;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;

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
    public static List<String> parseTableName(String sql) {
        TableNameCollector collector = new TableNameCollector();
        try {
            return new ArrayList<>(collector.parseTableName(sql));
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
