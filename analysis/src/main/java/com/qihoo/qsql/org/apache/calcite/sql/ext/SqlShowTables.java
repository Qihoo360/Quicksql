package com.qihoo.qsql.org.apache.calcite.sql.ext;

import com.google.common.collect.Lists;
import com.qihoo.qsql.org.apache.calcite.sql.*;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

//Updated by qsql-team
public class SqlShowTables extends SqlCall {

    private final SqlIdentifier db;
    private final SqlNode likePattern;

    public SqlShowTables(SqlParserPos pos, SqlIdentifier db, SqlNode likePattern) {
        super(pos);
        this.db = db;
        this.likePattern = likePattern;
    }

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW_TABLES", SqlKind.OTHER) {
                @Override
                public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlShowTables(pos, (SqlIdentifier) operands[0], operands[1]);
                }
            };


    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> opList = Lists.newArrayList();
        opList.add(db);
        opList.add(likePattern);
        return opList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW");
        writer.keyword("TABLES");
        if (db != null) {
            writer.keyword("FROM");
            db.unparse(writer, leftPrec, rightPrec);
        }
        if (likePattern != null) {
            writer.keyword("LIKE");
            likePattern.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlIdentifier getDb() {
        return db;
    }

    public SqlNode getLikePattern() {
        return likePattern;
    }
}
