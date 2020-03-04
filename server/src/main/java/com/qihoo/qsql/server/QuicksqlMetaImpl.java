package com.qihoo.qsql.server;

import com.google.common.collect.ImmutableList;
import com.qihoo.qsql.client.QuicksqlConnectionImpl;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;


public class QuicksqlMetaImpl extends MetaImpl {

    public QuicksqlMetaImpl(QuicksqlConnectionImpl connection) {
        super(connection);
        this.connProps
            .setAutoCommit(false)
            .setReadOnly(false)
            .setTransactionIsolation(Connection.TRANSACTION_NONE);
        this.connProps.setDirty(false);
    }


    @Override public void closeStatement(StatementHandle h) {}

    QuicksqlConnectionImpl getConnection() {
        return (QuicksqlConnectionImpl) connection;
    }

    @Override public StatementHandle prepare(ConnectionHandle ch, String sql,
        long maxRowCount) {
        StatementHandle result = super.createStatement(ch);
        result.signature = getConnection().mockPreparedSignature(sql);
        return result;
    }

    @SuppressWarnings("deprecation")
    @Override public ExecuteResult prepareAndExecute(StatementHandle h,
        String sql, long maxRowCount, PrepareCallback callback) {
        return prepareAndExecute(h, sql, maxRowCount, -1, callback);
    }

    @Override public ExecuteResult prepareAndExecute(StatementHandle h,
        String sql, long maxRowCount, int maxRowsInFirstFrame,
        PrepareCallback callback) {
        try {
            synchronized (callback.getMonitor()) {
                callback.clear();
                final QuicksqlConnectionImpl connection = getConnection();
                // h.signature = connection.mockPreparedSignature(sql);
                callback.assign(h.signature, null, -1);
            }
            callback.execute();
            final MetaResultSet metaResultSet =
                MetaResultSet.create(h.connectionId, h.id, false, h.signature, null);
            return new ExecuteResult(ImmutableList.of(metaResultSet));

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // TODO: share code with prepare and createIterable
    }

    @Override public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) {
        return null;
    }


    @Deprecated
    @Override public ExecuteResult execute(StatementHandle h,
        List<TypedValue> parameterValues, long maxRowCount) {
        final MetaResultSet metaResultSet = MetaResultSet.create(h.connectionId, h.id, false, h.signature, null);
        return new ExecuteResult(Collections.singletonList(metaResultSet));    }

    @Override public ExecuteResult execute(StatementHandle h,
        List<TypedValue> parameterValues, int maxRowsInFirstFrame) {
        final MetaResultSet metaResultSet = MetaResultSet.create(h.connectionId, h.id, false, h.signature, null);
        return new ExecuteResult(Collections.singletonList(metaResultSet));
    }

    @Override public ExecuteBatchResult prepareAndExecuteBatch(
        final StatementHandle h, List<String> sqlCommands) {
        return new ExecuteBatchResult(new long[]{});
    }

    @Override
    public ExecuteBatchResult executeBatch(StatementHandle h, List<List<TypedValue>> parameterValues) {
        return new ExecuteBatchResult(new long[]{});
    }

    public boolean syncResults(StatementHandle h, QueryState state, long offset) {
        return false;
    }

    @Override public void commit(ConnectionHandle ch) {
    }

    @Override public void rollback(ConnectionHandle ch) {
    }

}
