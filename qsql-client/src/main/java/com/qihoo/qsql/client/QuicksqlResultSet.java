package com.qihoo.qsql.client;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

/**
 * Implementation of {@link ResultSet} for the QuickSql engine.
 */
public class QuicksqlResultSet extends AvaticaResultSet {

    /**
     * Creates a QuicksqlResultSet.
     */
    public QuicksqlResultSet(AvaticaStatement statement,
        Meta.Signature calciteSignature,
        ResultSetMetaData resultSetMetaData, TimeZone timeZone,
        Meta.Frame firstFrame) throws SQLException {
        super(statement, null, calciteSignature, resultSetMetaData, timeZone, firstFrame);
    }
    @Override
    protected AvaticaResultSet execute() throws SQLException {
        return this;
    }

    public static class QueryResult {

        public final List<ColumnMetaData> columnMeta;
        public final Iterable<Object> iterable;

        public QueryResult(List<ColumnMetaData> columnMeta, Iterable<Object> iterable) {
            this.columnMeta = columnMeta;
            this.iterable = iterable;
        }
    }
}

// End QuicksqlResultSet.java
