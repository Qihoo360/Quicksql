package com.qihoo.qsql.server;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.UnregisteredDriver;

public class QuicksqlConnectionImpl extends AvaticaConnection {
    /**
     * Creates a QuicksqlConnectionImpl.
     *
     * <p>Not public; method is called only from the driver.</p>
     *
     * @param driver Driver
     * @param factory Factory for JDBC objects
     * @param url Server URL
     * @param info Other connection properties
     */
    protected QuicksqlConnectionImpl(UnregisteredDriver driver, AvaticaFactory factory,
        String url, Properties info) {
        super(driver, factory, url, info);
    }


    @Override public QuicksqlStatement createStatement(int resultSetType,
        int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return (QuicksqlStatement) super.createStatement(resultSetType,
            resultSetConcurrency, resultSetHoldability);
    }

    public String getInfoByName(String name){
        return this.info.getProperty(name);
    }

    public Signature mockPreparedSignature(String sql) {
        List<AvaticaParameter> params = new ArrayList<AvaticaParameter>();
        int startIndex = 0;
        while (sql.indexOf("?", startIndex) >= 0) {
            AvaticaParameter param = new AvaticaParameter(false, 0, 0, 0, null, null, null);
            params.add(param);
            startIndex = sql.indexOf("?", startIndex) + 1;
        }

        ArrayList<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
        Map<String, Object> internalParams = Collections.<String, Object> emptyMap();

        return new Meta.Signature(columns, sql, params, internalParams, CursorFactory.ARRAY, Meta.StatementType.SELECT);
    }
}