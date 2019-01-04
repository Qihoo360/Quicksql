package org.apache.calcite.sql.ext;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

/**
 * Sql parser tree node to represent <code>USE SCHEMA</code> statement.
 */
public class SqlUseSchema extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("USE_SCHEMA", SqlKind.OTHER){
                @Override
                public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlUseSchema(pos, (SqlIdentifier) operands[0]);
                }
            };

    private SqlIdentifier schema;

    public SqlUseSchema(SqlParserPos pos, SqlIdentifier schema) {
        super(pos);
        this.schema = schema;
        assert schema != null;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList((SqlNode)schema);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("USE");
        schema.unparse(writer, leftPrec, rightPrec);
    }

    /**
     * Get the schema name. A schema identifier can contain more than one level of schema.
     * Ex: "dfs.home" identifier contains two levels "dfs" and "home".
     * @return schemas combined with "."
     */
    public String getSchema() {
        return schema.toString();
    }
}