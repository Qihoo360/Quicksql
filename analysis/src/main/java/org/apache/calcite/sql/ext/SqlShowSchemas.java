package org.apache.calcite.sql.ext;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Sql parse tree node to represent statement:
 * SHOW {DATABASES | SCHEMAS} [LIKE 'pattern' | WHERE expr]
 */
public class SqlShowSchemas extends SqlCall {

    private final SqlNode likePattern;
    private final SqlNode whereClause;

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW_SCHEMAS", SqlKind.OTHER) {
                @Override
                public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlShowSchemas(pos, operands[0], operands[1]);
                }
            };

    public SqlShowSchemas(SqlParserPos pos, SqlNode likePattern, SqlNode whereClause) {
        super(pos);
        this.likePattern = likePattern;
        this.whereClause = whereClause;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> opList = Lists.newArrayList();
        opList.add(likePattern);
        opList.add(whereClause);
        return opList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW");
        writer.keyword("SCHEMAS");
        if (likePattern != null) {
            writer.keyword("LIKE");
            likePattern.unparse(writer, leftPrec, rightPrec);
        }
        if (whereClause != null) {
            whereClause.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlNode getLikePattern() { return likePattern; }
    public SqlNode getWhereClause() { return whereClause; }

}