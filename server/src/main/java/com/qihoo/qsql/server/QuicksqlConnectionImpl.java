package com.qihoo.qsql.server;

import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
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

    /**
     * client query connect params
     */
    public static class ClientConnectParam {
        private String jdbcUrl;
        private String user;
        private String password;
        private boolean isDirectQuery = false;

        private int acceptedResultsNum = 100000;
        private RunnerType runnerType;
        private String schemaPath;
        private String appName;
        private String responseUrl;

        public ClientConnectParam() {
        }

        public ClientConnectParam(String jdbcUrl, String user, String password) {
            this.jdbcUrl = jdbcUrl;
            this.user = user;
            this.password = password;
            this.isDirectQuery = true;
        }

        public int getAcceptedResultsNum() {
            return acceptedResultsNum;
        }

        public void setAcceptedResultsNum(int acceptedResultsNum) {
            this.acceptedResultsNum = acceptedResultsNum;
        }

        public RunnerType getRunnerType() {
            return runnerType;
        }

        public void setRunnerType(RunnerType runnerType) {
            this.runnerType = runnerType;
        }

        public String getSchemaPath() {
            return schemaPath;
        }

        public void setSchemaPath(String schemaPath) {
            this.schemaPath = schemaPath;
        }

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public String getResponseUrl() {
            return responseUrl;
        }

        public void setResponseUrl(String responseUrl) {
            this.responseUrl = responseUrl;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }

        public boolean isDirectQuery() {
            return isDirectQuery;
        }
    }
}