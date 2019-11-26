package com.qihoo.qsql.org.apache.calcite.sql.ext;

import java.util.List;
import com.qihoo.qsql.org.apache.calcite.sql.SqlCall;
import com.qihoo.qsql.org.apache.calcite.sql.SqlIdentifier;
import com.qihoo.qsql.org.apache.calcite.sql.SqlKind;
import com.qihoo.qsql.org.apache.calcite.sql.SqlLiteral;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNodeList;
import com.qihoo.qsql.org.apache.calcite.sql.SqlOperator;
import com.qihoo.qsql.org.apache.calcite.sql.SqlSpecialOperator;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParserPos;
import com.qihoo.qsql.org.apache.calcite.util.ImmutableNullableList;

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
