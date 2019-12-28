package com.qihoo.qsql.exec;

import com.qihoo.qsql.metadata.ColumnValue;
import com.qihoo.qsql.metadata.MetadataClient;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import com.qihoo.qsql.plan.TableNameCollector;
import dnl.utils.text.table.TextTable;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import com.qihoo.qsql.org.apache.calcite.sql.SqlDescribeTable;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.ext.SqlShowSchemas;
import com.qihoo.qsql.org.apache.calcite.sql.ext.SqlShowTables;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShowDbHandler implements DdlOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataPostman.class);

    private static final Integer HEAD = 1;

    private static final Integer TAIL = 2;

    private static final Integer HEAD_TAIL = 0;


    /**
     * list tables from tbls of meta-data database.
     */
    public static void showTable(SqlShowTables sqlNode) throws SQLException {
        String tableName = sqlNode.getLikePattern() != null ? sqlNode.getLikePattern().toString().replaceAll("\'", "") :
            "";
        MetadataClient metadataClient = new MetadataClient();
        List<TableValue> tableSchemas = metadataClient.getTableSchemaByTbl(tableName);
        //for (TableValue tableValue : tableSchemas) {
        //    System.out.println(tableValue.getTblName());
        //}
        String[][] tableNames = new String[tableSchemas.size()][1];
        for (int i = 0; i < tableSchemas.size(); i++) {
            tableNames[i][0] = tableSchemas.get(i).getTblName();
        }
        String[] columnNames = {"name",};
        TextTable tt = new TextTable(columnNames, tableNames);
        tt.printTable();
    }

    /**
     * list databases information from dbs tables of meta-data .
     */
    public static void showDataBase(SqlNode sqlNode) throws SQLException {
        MetadataClient metadataClient = new MetadataClient();
        List<DatabaseValue> databaseList = metadataClient.getDataBaseList();
        String[][] tableNames = new String[databaseList.size()][1];
        for (int i = 0; i < databaseList.size(); i++) {
            tableNames[i][0] = databaseList.get(i).getName();
        }
        String[] columnNames = {"name",};
        TextTable tt = new TextTable(columnNames, tableNames);
        tt.printTable();
    }

    /**
     * list table fields information from columns table of meta-data.
     */
    public static void describeTable(SqlDescribeTable sqlNode) throws SQLException {
        String tableName = sqlNode.getTable().toString();
        String columnFilter = sqlNode.getColumn() != null ? sqlNode.getColumn().toString().replaceAll("'", "") : "";
        MetadataClient metadataClient = new MetadataClient();
        List<TableValue> tableList = metadataClient.getTableSchema(tableName);
        Long tableId = tableList.get(0).getTblId();
        List<ColumnValue> columnList = metadataClient.getFieldsSchema(tableId);
        if (StringUtils.isNotEmpty(columnFilter)) {
            int pos = likePosition(columnFilter);
            String likePattern = columnFilter.replaceAll("%", "");
            columnList = columnList.stream().filter(col -> {
                switch (pos) {
                    case 0:
                        return col.getColumnName().contains(likePattern);
                    case 1:
                        return col.getColumnName().startsWith(likePattern);
                    case 2:
                        return col.getColumnName().endsWith(likePattern);
                    default:
                        return col.getColumnName().contains(likePattern);
                }
            }).collect(Collectors.toList());
        }
        String[][] data = new String[columnList.size()][2];
        String[] columnNames = {"name", "type"};
        for (int i = 0; i < columnList.size(); i++) {
            data[i][0] = columnList.get(i).getColumnName();
            data[i][1] = columnList.get(i).getTypeName();
        }
        TextTable tt = new TextTable(columnNames, data);
        tt.printTable();
    }


    /**
     * get percentage sign position.
     *
     * @param filter fitler condition.
     */
    private static Integer likePosition(String filter) {
        if (filter.startsWith("%") && filter.endsWith("%")) {
            return HEAD_TAIL;
        } else if (filter.startsWith("%")) {
            return TAIL;
        } else if (filter.endsWith("%")) {
            return HEAD;
        } else {
            return HEAD_TAIL;
        }
    }

    @Override
    public void execute(String sql) throws SQLException {
        TableNameCollector collector = new TableNameCollector();
        try {
            SqlNode sqlNode = collector.parseSql(sql);
            if (sqlNode instanceof SqlShowSchemas) {
                showDataBase(sqlNode);
            } else if (sqlNode instanceof SqlShowTables) {
                showTable((SqlShowTables) sqlNode);
            } else if (sqlNode instanceof SqlDescribeTable) {
                describeTable((SqlDescribeTable) sqlNode);
            } else {
                System.out.println("No matched.");
            }
        } catch (SqlParseException ex) {
            LOGGER.error("[ShowDbHandler-showTable] failed to parse sql");
            ex.printStackTrace();
        }
    }
}
