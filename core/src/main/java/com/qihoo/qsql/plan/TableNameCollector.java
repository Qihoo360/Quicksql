package com.qihoo.qsql.plan;

import com.qihoo.qsql.exception.ParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.ext.SqlInsertOutput;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/**
 * Parse SQL line and extract the table name in it.
 */
public class TableNameCollector implements SqlVisitor<QueryTables> {

    private Set<String> withTempTables = new HashSet<>();
    private QueryTables tableNames = new QueryTables();
    //TODO extract SqlParser to correspond with calcite-core
    private SqlConformance conformance = SqlConformanceEnum.MYSQL_5;
    private Quoting quoting = Quoting.BACK_TICK;

    private SqlParser.Config config = SqlParser
        .configBuilder()
        .setConformance(conformance)
        .setQuoting(quoting)
        .setQuotedCasing(Casing.UNCHANGED)
        .setUnquotedCasing(Casing.UNCHANGED)
        .build();

    /**
     * Get table names from sql.
     *
     * @param sql SQL line
     * @return List of TableName
     */
    public QueryTables parseTableName(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();
        return validateTableName(sqlNode.accept(this));
    }

    @Override
    public QueryTables visit(SqlLiteral sqlLiteral) {
        return tableNames;
    }

    @Override
    public QueryTables visit(SqlCall sqlCall) {
        if (sqlCall instanceof SqlInsertOutput) {
            tableNames.isDmlActually();
            ((SqlInsertOutput) sqlCall).getSelect().accept(this);
        }

        if (sqlCall instanceof SqlSelect) {
            ((SqlSelect) sqlCall).getSelectList().accept(this);
            if (((SqlSelect) sqlCall).getFrom() != null) {
                ((SqlSelect) sqlCall).getFrom().accept(this);
            }
            if (((SqlSelect) sqlCall).getWhere() instanceof SqlBasicCall) {
                List<SqlNode> operands =
                    ((SqlBasicCall) ((SqlSelect) sqlCall).getWhere()).getOperandList();
                for (SqlNode operand : operands) {
                    if (!(operand instanceof SqlIdentifier)) {
                        operand.accept(this);
                    }
                }
            }
        }

        if (sqlCall instanceof SqlWith) {
            ((SqlWith) sqlCall).withList.accept(this);
            ((SqlWith) sqlCall).body.accept(this);
        }

        if (sqlCall instanceof SqlJoin) {
            ((SqlJoin) sqlCall).getLeft().accept(this);
            ((SqlJoin) sqlCall).getRight().accept(this);
        }

        if (sqlCall instanceof SqlBasicCall) {
            visitBasicCall((SqlBasicCall) sqlCall);
        }

        if (sqlCall instanceof SqlOrderBy) {
            ((SqlOrderBy) sqlCall).query.accept(this);
        }

        return tableNames;
    }

    @Override
    public QueryTables visit(SqlNodeList sqlNodeList) {
        sqlNodeList.iterator().forEachRemaining((entry) -> {
            if (entry instanceof SqlSelect) {
                entry.accept(this);
            } else if (entry instanceof SqlWithItem) {
                //TODO caution db.table query
                List<String> names = ((SqlWithItem) entry).name.names;
                if (!names.isEmpty()) {
                    withTempTables.add(names.get(names.size() - 1));
                }
                ((SqlWithItem) entry).query.accept(this);
            } else if (entry instanceof SqlBasicCall) {
                String kind = ((SqlBasicCall) entry).getOperator().getName();
                if ("AS".equalsIgnoreCase(kind)
                    && ((SqlBasicCall) entry).operand(0) instanceof SqlSelect) {
                    entry.accept(this);
                }
            }
        });
        return tableNames;
    }

    @Override
    public QueryTables visit(SqlIdentifier sqlIdentifier) {
        if (sqlIdentifier.names.size() == 0) {
            return tableNames;
        }

        tableNames.add(sqlIdentifier.toString());
        return tableNames;
    }

    @Override
    public QueryTables visit(SqlDataTypeSpec sqlDataTypeSpec) {
        return tableNames;
    }

    @Override
    public QueryTables visit(SqlDynamicParam sqlDynamicParam) {
        return tableNames;
    }

    @Override
    public QueryTables visit(SqlIntervalQualifier sqlIntervalQualifier) {
        return tableNames;
    }

    private void visitBasicCall(SqlBasicCall sqlCall) {
        if (sqlCall.getOperator() instanceof SqlAsOperator && (sqlCall).operands.length == 2) {
            if ((sqlCall).operands[0] instanceof SqlIdentifier
                && (sqlCall).operands[1] instanceof SqlIdentifier) {
                (sqlCall).operands[0].accept(this);
            } else if (!((sqlCall).operands[0] instanceof SqlIdentifier)) {
                (sqlCall).operands[0].accept(this);
            }
        } else {
            Arrays.stream((sqlCall).operands).forEach((node) -> {
                if (node instanceof SqlSelect) {
                    if (((SqlSelect) node).getFrom() != null) {
                        ((SqlSelect) node).getFrom().accept(this);
                    }
                }

                if (node instanceof SqlBasicCall) {
                    visitBasicCall((SqlBasicCall) node);
                }
            });
        }
    }

    private QueryTables validateTableName(QueryTables tableNames) {
        for (String tableName : tableNames.tableNames) {
            if (tableName.split("\\.", -1).length > 2) {
                throw new ParseException("Qsql only support structure like dbName.tableName,"
                    + " and there is a unsupported tableName here: " + tableName);
            }
        }
        tableNames.tableNames.removeIf((item) -> withTempTables.contains(item));
        return tableNames;
    }

    /**
     * generate SqlNode according sql.
     */
    public SqlNode parseSql(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();
        return sqlNode;
    }
}
