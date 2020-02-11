package com.qihoo.qsql.org.apache.calcite.sql.ext;

import com.qihoo.qsql.org.apache.calcite.sql.*;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParserPos;
import com.qihoo.qsql.org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

//Updated by qsql-team
public class SqlInsertOutput extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
        new SqlSpecialOperator("WRITE OUTPUT", SqlKind.OTHER){
            @Override
            public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                return new SqlInsertOutput(pos, operands[0],
                    (SqlIdentifier) operands[1], (SqlNodeList) operands[2], operands[3]);
            }
        };

    private SqlNode path;
    private SqlIdentifier dataSource;
    private SqlNode select;
    private SqlNodeList columnsList;

    public SqlInsertOutput(
        SqlParserPos pos, SqlNode path,
        SqlIdentifier dataSource, SqlNodeList columnsList, SqlNode select) {
        super(pos);
        this.path = path;
        this.select = select;
        this.dataSource = dataSource;
        this.columnsList = columnsList;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(path, select);
    }

    public SqlNode getPath() {
        return path;
    }

    public SqlIdentifier getDataSource() {
        return dataSource;
    }

    public SqlNode getSelect() {
        return select;
    }

    public SqlNodeList getColumnsList() {
        return columnsList;
    }
}
