package com.qihoo.qsql.server;

import static org.apache.calcite.avatica.remote.MetricsHelper.concat;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.gson.JsonObject;
import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.client.QuicksqlConnectionImpl;
import com.qihoo.qsql.client.QuicksqlResultSet;
import com.qihoo.qsql.client.QuicksqlResultSet.QueryResult;
import com.qihoo.qsql.org.apache.calcite.tools.YmlUtils;
import com.qihoo.qsql.utils.HttpUtils;
import com.qihoo.qsql.utils.SqlUtil;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.ColumnMetaData.ScalarType;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchConnectionException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.metrics.Gauge;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.remote.ProtobufMeta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.Cursor;
import org.apache.calcite.avatica.util.Unsafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.spark.SparkException;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Meta} upon an existing JDBC data source.
 */
public class QuicksqlServerMeta implements ProtobufMeta {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuicksqlServerMeta.class);

    private static final String CONN_CACHE_KEY_BASE = "avatica.connectioncache";

    private static final String STMT_CACHE_KEY_BASE = "avatica.statementcache";

    /**
     * Special value for {@code Statement#getLargeMaxRows()} that means fetch an unlimited number of rows in a single
     * batch.
     *
     * <p>Any other negative value will return an unlimited number of rows but
     * will do it in the default batch size, namely 100.
     */
    public static final int UNLIMITED_COUNT = -2;

    final Calendar calendar = Unsafe.localCalendar();

    /**
     * Generates ids for statements. The ids are unique across all connections created by this QuicksqlServerMeta.
     */
    private final AtomicInteger statementIdGenerator = new AtomicInteger();

    private final String url;
    private final Properties info;
    private final Cache<String, Connection> connectionCache;
    private final Cache<Integer, StatementInfo> statementCache;
    private final MetricsSystem metrics;

    /**
     * Creates a QuicksqlServerMeta.
     *
     * @param url a database url of the form
     * <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
     */
    public QuicksqlServerMeta(String url) throws SQLException {
        this(url, new Properties());
    }

    /**
     * Creates a QuicksqlServerMeta.
     *
     * @param url a database url of the form
     * <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
     * @param user the database user on whose behalf the connection is being made
     * @param password the user's password
     */
    public QuicksqlServerMeta(final String url, final String user, final String password)
        throws SQLException {
        this(url, new Properties() {
            {
                put("user", user);
                put("password", password);
            }
        });
    }

    public QuicksqlServerMeta(String url, Properties info) throws SQLException {
        this(url, info, NoopMetricsSystem.getInstance());
    }

    /**
     * Creates a QuicksqlServerMeta.
     *
     * @param url a database url of the form
     * <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
     * @param info a list of arbitrary string tag/value pairs as connection arguments; normally at least a "user" and
     * "password" property should be included
     */
    public QuicksqlServerMeta(String url, Properties info, MetricsSystem metrics) throws SQLException {
        this.url = url;
        this.info = info;
        this.metrics = Objects.requireNonNull(metrics);

        int concurrencyLevel = Integer.parseInt(
            info.getProperty(ConnectionCacheSettings.CONCURRENCY_LEVEL.key(),
                ConnectionCacheSettings.CONCURRENCY_LEVEL.defaultValue()));
        int initialCapacity = Integer.parseInt(
            info.getProperty(ConnectionCacheSettings.INITIAL_CAPACITY.key(),
                ConnectionCacheSettings.INITIAL_CAPACITY.defaultValue()));
        long maxCapacity = Long.parseLong(
            info.getProperty(ConnectionCacheSettings.MAX_CAPACITY.key(),
                ConnectionCacheSettings.MAX_CAPACITY.defaultValue()));
        long connectionExpiryDuration = Long.parseLong(
            info.getProperty(ConnectionCacheSettings.EXPIRY_DURATION.key(),
                ConnectionCacheSettings.EXPIRY_DURATION.defaultValue()));
        TimeUnit connectionExpiryUnit = TimeUnit.valueOf(
            info.getProperty(ConnectionCacheSettings.EXPIRY_UNIT.key(),
                ConnectionCacheSettings.EXPIRY_UNIT.defaultValue()));
        this.connectionCache = CacheBuilder.newBuilder()
            .concurrencyLevel(concurrencyLevel)
            .initialCapacity(initialCapacity)
            .maximumSize(maxCapacity)
            .expireAfterAccess(connectionExpiryDuration, connectionExpiryUnit)
            .removalListener(new ConnectionExpiryHandler())
            .build();
        LOGGER.debug("instantiated connection cache: {}", connectionCache.stats());

        concurrencyLevel = Integer.parseInt(
            info.getProperty(StatementCacheSettings.CONCURRENCY_LEVEL.key(),
                StatementCacheSettings.CONCURRENCY_LEVEL.defaultValue()));
        initialCapacity = Integer.parseInt(
            info.getProperty(StatementCacheSettings.INITIAL_CAPACITY.key(),
                StatementCacheSettings.INITIAL_CAPACITY.defaultValue()));
        maxCapacity = Long.parseLong(
            info.getProperty(StatementCacheSettings.MAX_CAPACITY.key(),
                StatementCacheSettings.MAX_CAPACITY.defaultValue()));
        connectionExpiryDuration = Long.parseLong(
            info.getProperty(StatementCacheSettings.EXPIRY_DURATION.key(),
                StatementCacheSettings.EXPIRY_DURATION.defaultValue()));
        connectionExpiryUnit = TimeUnit.valueOf(
            info.getProperty(StatementCacheSettings.EXPIRY_UNIT.key(),
                StatementCacheSettings.EXPIRY_UNIT.defaultValue()));
        this.statementCache = CacheBuilder.newBuilder()
            .concurrencyLevel(concurrencyLevel)
            .initialCapacity(initialCapacity)
            .maximumSize(maxCapacity)
            .expireAfterAccess(connectionExpiryDuration, connectionExpiryUnit)
            .removalListener(new StatementExpiryHandler())
            .build();

        LOGGER.debug("instantiated statement cache: {}", statementCache.stats());

        // Register some metrics
        this.metrics.register(concat(
            QuicksqlServerMeta.class, "ConnectionCacheSize"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return connectionCache.size();
            }
        });

        this.metrics.register(concat(QuicksqlServerMeta.class, "StatementCacheSize"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statementCache.size();
            }
        });
    }


    // For testing purposes
    protected AtomicInteger getStatementIdGenerator() {
        return statementIdGenerator;
    }

    // For testing purposes
    protected Cache<String, Connection> getConnectionCache() {
        return connectionCache;
    }

    // For testing purposes
    protected Cache<Integer, StatementInfo> getStatementCache() {
        return statementCache;
    }

    /**
     * Converts from JDBC metadata to Avatica columns.
     */
    protected static List<ColumnMetaData>
    columns(ResultSetMetaData metaData) throws SQLException {
        if (metaData == null) {
            return Collections.emptyList();
        }
        final List<ColumnMetaData> columns = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            int typeId = 0;
            switch (metaData.getColumnType(i)) {
                case -7:
                    typeId = Types.BOOLEAN;
                    break;
                case -1:
                    typeId = 12;
                    break;
                default:
                    typeId = metaData.getColumnType(i);
            }
            String columnClassName = metaData.getColumnClassName(i);
            final SqlType sqlType = SqlType.valueOf(typeId);
            final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(sqlType.internal);
            final ColumnMetaData.AvaticaType t;
            if (sqlType == SqlType.ARRAY || sqlType == SqlType.STRUCT || sqlType == SqlType.MULTISET) {
                ColumnMetaData.AvaticaType arrayValueType = ColumnMetaData.scalar(Types.JAVA_OBJECT,
                    metaData.getColumnTypeName(i), ColumnMetaData.Rep.OBJECT);
                t = ColumnMetaData.array(arrayValueType, metaData.getColumnTypeName(i), rep);
            } else {
                t = ColumnMetaData.scalar(typeId, metaData.getColumnTypeName(i), rep);
            }
            ColumnMetaData md =
                new ColumnMetaData(i - 1, metaData.isAutoIncrement(i),
                    metaData.isCaseSensitive(i), metaData.isSearchable(i),
                    metaData.isCurrency(i), metaData.isNullable(i),
                    metaData.isSigned(i), metaData.getColumnDisplaySize(i),
                    metaData.getColumnLabel(i), metaData.getColumnName(i),
                    metaData.getSchemaName(i), metaData.getPrecision(i),
                    metaData.getScale(i), metaData.getTableName(i),
                    metaData.getCatalogName(i), t, metaData.isReadOnly(i),
                    metaData.isWritable(i), metaData.isDefinitelyWritable(i),
                    columnClassName);
            columns.add(md);
        }
        return columns;
    }

    /**
     * Converts from JDBC metadata to Avatica parameters
     */
    protected static List<AvaticaParameter> parameters(ParameterMetaData metaData)
        throws SQLException {
        if (metaData == null) {
            return Collections.emptyList();
        }
        final List<AvaticaParameter> params = new ArrayList<>();
        for (int i = 1; i <= metaData.getParameterCount(); i++) {
            params.add(
                new AvaticaParameter(metaData.isSigned(i), metaData.getPrecision(i),
                    metaData.getScale(i), metaData.getParameterType(i),
                    metaData.getParameterTypeName(i),
                    metaData.getParameterClassName(i), "?" + i));
        }
        return params;
    }

    protected static Signature signature(ResultSetMetaData metaData,
        ParameterMetaData parameterMetaData, String sql,
        Meta.StatementType statementType) throws SQLException {
        final CursorFactory cf = CursorFactory.LIST;  // because QuicksqlServerResultSet#frame
        return new Signature(columns(metaData), sql, parameters(parameterMetaData),
            null, cf, statementType);
    }

    protected static Signature signature(ResultSetMetaData metaData)
        throws SQLException {
        return signature(metaData, null, null, null);
    }

    public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle ch) {
        try {
            final Map<DatabaseProperty, Object> map = new HashMap<>();
            final Connection conn = getConnection(ch.id);
            final DatabaseMetaData metaData = conn.getMetaData();
            for (DatabaseProperty p : DatabaseProperty.values()) {
                addProperty(map, metaData, p);
            }
            return map;
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    private static Object addProperty(Map<DatabaseProperty, Object> map,
        DatabaseMetaData metaData, DatabaseProperty p) throws SQLException {
        Object propertyValue;
        if (p.isJdbc) {
            try {
                propertyValue = p.method.invoke(metaData);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw propagate(e);
            }
        } else {
            propertyValue = p.defaultValue;
        }

        return map.put(p, propertyValue);
    }

    public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat tableNamePattern, List<String> typeList) {
        try {
            final ResultSet rs =
                getConnection(ch.id).getMetaData().getTables(catalog, schemaPattern.s,
                    tableNamePattern.s, toArray(typeList));
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    /**
     * Registers a StatementInfo for the given ResultSet, returning the id under which it is registered. This should be
     * used for metadata ResultSets, which have an implicit statement created.
     */
    private int registerMetaStatement(ResultSet rs) throws SQLException {
        final int id = statementIdGenerator.getAndIncrement();
        StatementInfo statementInfo = new StatementInfo(rs.getStatement());
        statementInfo.setResultSet(rs);
        statementCache.put(id, statementInfo);
        return id;
    }

    public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat tableNamePattern, Pat columnNamePattern) {
        try {
            final ResultSet rs =
                getConnection(ch.id).getMetaData().getColumns(catalog, schemaPattern.s,
                    tableNamePattern.s, columnNamePattern.s);
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
        try {
            final ResultSet rs =
                getConnection(ch.id).getMetaData().getSchemas(catalog, schemaPattern.s);
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getCatalogs(ConnectionHandle ch) {
        try {
            final ResultSet rs = getConnection(ch.id).getMetaData().getCatalogs();
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getTableTypes(ConnectionHandle ch) {
        try {
            final ResultSet rs = getConnection(ch.id).getMetaData().getTableTypes();
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getProcedures(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat procedureNamePattern) {
        try {
            final ResultSet rs =
                getConnection(ch.id).getMetaData().getProcedures(catalog, schemaPattern.s,
                    procedureNamePattern.s);
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getProcedureColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat procedureNamePattern, Pat columnNamePattern) {
        try {
            final ResultSet rs =
                getConnection(ch.id).getMetaData().getProcedureColumns(catalog,
                    schemaPattern.s, procedureNamePattern.s, columnNamePattern.s);
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getColumnPrivileges(ConnectionHandle ch, String catalog, String schema,
        String table, Pat columnNamePattern) {
        try {
            final ResultSet rs =
                getConnection(ch.id).getMetaData().getColumnPrivileges(catalog, schema,
                    table, columnNamePattern.s);
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getTablePrivileges(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat tableNamePattern) {
        try {
            final ResultSet rs =
                getConnection(ch.id).getMetaData().getTablePrivileges(catalog,
                    schemaPattern.s, tableNamePattern.s);
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getBestRowIdentifier(ConnectionHandle ch, String catalog, String schema,
        String table, int scope, boolean nullable) {
        LOGGER.trace("getBestRowIdentifier catalog:{} schema:{} table:{} scope:{} nullable:{}", catalog,
            schema, table, scope, nullable);
        try {
            final ResultSet rs =
                getConnection(ch.id).getMetaData().getBestRowIdentifier(catalog, schema,
                    table, scope, nullable);
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getVersionColumns(ConnectionHandle ch, String catalog, String schema,
        String table) {
        LOGGER.trace("getVersionColumns catalog:{} schema:{} table:{}", catalog, schema, table);
        try {
            final ResultSet rs =
                getConnection(ch.id).getMetaData().getVersionColumns(catalog, schema, table);
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getPrimaryKeys(ConnectionHandle ch, String catalog, String schema,
        String table) {
        LOGGER.trace("getPrimaryKeys catalog:{} schema:{} table:{}", catalog, schema, table);
        try {
            final ResultSet rs =
                getConnection(ch.id).getMetaData().getPrimaryKeys(catalog, schema, table);
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getImportedKeys(ConnectionHandle ch, String catalog, String schema,
        String table) {
        return null;
    }

    public MetaResultSet getExportedKeys(ConnectionHandle ch, String catalog, String schema,
        String table) {
        return null;
    }

    public MetaResultSet getCrossReference(ConnectionHandle ch, String parentCatalog,
        String parentSchema, String parentTable, String foreignCatalog,
        String foreignSchema, String foreignTable) {
        return null;
    }

    public MetaResultSet getTypeInfo(ConnectionHandle ch) {
        try {
            final ResultSet rs = getConnection(ch.id).getMetaData().getTypeInfo();
            int stmtId = registerMetaStatement(rs);
            return QuicksqlServerResultSet.create(ch.id, stmtId, rs);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    public MetaResultSet getIndexInfo(ConnectionHandle ch, String catalog, String schema,
        String table, boolean unique, boolean approximate) {
        return null;
    }

    public MetaResultSet getUDTs(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat typeNamePattern, int[] types) {
        return null;
    }

    public MetaResultSet getSuperTypes(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat typeNamePattern) {
        return null;
    }

    public MetaResultSet getSuperTables(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat tableNamePattern) {
        return null;
    }

    public MetaResultSet getAttributes(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat typeNamePattern, Pat attributeNamePattern) {
        return null;
    }

    public MetaResultSet getClientInfoProperties(ConnectionHandle ch) {
        return null;
    }

    public MetaResultSet getFunctions(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat functionNamePattern) {
        return null;
    }

    public MetaResultSet getFunctionColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat functionNamePattern, Pat columnNamePattern) {
        return null;
    }

    public MetaResultSet getPseudoColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
        Pat tableNamePattern, Pat columnNamePattern) {
        return null;
    }

    public Iterable<Object> createIterable(StatementHandle handle, QueryState state,
        Signature signature, List<TypedValue> parameterValues, Frame firstFrame) {
        return null;
    }

    protected Connection getConnection(String id) throws SQLException {
        if (id == null) {
            throw new NullPointerException("Connection id is null.");
        }
        Connection conn = connectionCache.getIfPresent(id);
        if (conn == null) {
            throw new NoSuchConnectionException("Connection not found: invalid id, closed, or expired: "
                + id);
        }
        return conn;
    }

    public StatementHandle createStatement(ConnectionHandle ch) {
        try {
            final Connection conn = getConnection(ch.id);
            final Statement statement = conn.createStatement();
            final int id = statementIdGenerator.getAndIncrement();
            statementCache.put(id, new StatementInfo(statement));
            StatementHandle h = new StatementHandle(ch.id, id, null);
            LOGGER.trace("created statement {}", h);
            return h;
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public void closeStatement(StatementHandle h) {
        StatementInfo info = statementCache.getIfPresent(h.id);
        if (info == null || info.statement == null) {
            LOGGER.debug("client requested close unknown statement {}", h);
            return;
        }
        LOGGER.trace("closing statement {}", h);
        try {
            ResultSet results = info.getResultSet();
            if (info.isResultSetInitialized() && null != results) {
                results.close();
            }
            info.statement.close();
        } catch (SQLException e) {
            throw propagate(e);
        } finally {
            statementCache.invalidate(h.id);
        }
    }

    @Override
    public void openConnection(ConnectionHandle ch,
        Map<String, String> info) {
        Properties fullInfo = new Properties();
        fullInfo.putAll(this.info);
        if (info != null) {
            fullInfo.putAll(info);
        }

        final ConcurrentMap<String, Connection> cacheAsMap = connectionCache.asMap();
        if (cacheAsMap.containsKey(ch.id)) {
            throw new RuntimeException("Connection already exists: " + ch.id);
        }
        // Avoid global synchronization of connection opening
        try {
            Connection conn = createConnection(url, fullInfo);
            Connection loadedConn = cacheAsMap.putIfAbsent(ch.id, conn);
            // Race condition: someone beat us to storing the connection in the cache.
            if (loadedConn != null) {
                conn.close();
                throw new RuntimeException("Connection already exists: " + ch.id);
            }
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    protected Connection createConnection(String url, Properties info) throws SQLException {
        return DriverManager.getConnection(url, info);
    }

    @Override
    public void closeConnection(ConnectionHandle ch) {
        Connection conn = connectionCache.getIfPresent(ch.id);
        if (conn == null) {
            LOGGER.debug("client requested close unknown connection {}", ch);
            return;
        }
        LOGGER.trace("closing connection {}", ch);
        try {
            conn.close();
        } catch (SQLException e) {
            throw propagate(e);
        } finally {
            connectionCache.invalidate(ch.id);
        }
    }

    protected void apply(Connection conn, ConnectionProperties connProps)
        throws SQLException {
        if (connProps.isAutoCommit() != null) {
            conn.setAutoCommit(connProps.isAutoCommit());
        }
        if (connProps.isReadOnly() != null) {
            conn.setReadOnly(connProps.isReadOnly());
        }
        if (connProps.getTransactionIsolation() != null) {
            conn.setTransactionIsolation(connProps.getTransactionIsolation());
        }
        if (connProps.getCatalog() != null) {
            conn.setCatalog(connProps.getCatalog());
        }
        if (connProps.getSchema() != null) {
            conn.setSchema(connProps.getSchema());
        }
    }

    @Override
    public ConnectionProperties connectionSync(ConnectionHandle ch,
        ConnectionProperties connProps) {
        LOGGER.trace("syncing properties for connection {}", ch);
        try {
            Connection conn = getConnection(ch.id);
            ConnectionPropertiesImpl props = new ConnectionPropertiesImpl().merge(connProps);
            if (props.isDirty()) {
                apply(conn, props);
                props.setDirty(false);
            }
            return props;
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    static <E extends Throwable> RuntimeException propagate(Throwable e) throws E {
        // We have nothing to add, so just throw the original exception
        throw (E) e;
    }

    public StatementHandle prepare(ConnectionHandle ch, String sql,
        long maxRowCount) {
        try {
            QuicksqlConnectionImpl qsqlConn = (QuicksqlConnectionImpl) getConnection(ch.id);
            PreparedStatement statement = qsqlConn.prepareStatement(sql);
            final int id = getStatementIdGenerator().getAndIncrement();
            Meta.StatementType statementType = null;
            if (statement.isWrapperFor(AvaticaPreparedStatement.class)) {
                final AvaticaPreparedStatement avaticaPreparedStatement;
                avaticaPreparedStatement =
                    statement.unwrap(AvaticaPreparedStatement.class);
                statementType = avaticaPreparedStatement.getStatementType();
            }
            setMaxRows(statement, maxRowCount);
            getStatementCache().put(id, new StatementInfo(statement));
            StatementHandle h = new StatementHandle(ch.id, id,
                signature(statement.getMetaData(), statement.getParameterMetaData(), sql, statementType));
            LOGGER.trace("prepared statement {}", h);
            return h;
        } catch (SQLException e) {
            e.printStackTrace();
            throw propagate(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
        long maxRowCount) throws NoSuchStatementException {
        return execute(h, parameterValues, AvaticaUtils.toSaturatedInt(maxRowCount));
    }

    @Override
    public ExecuteResult execute(StatementHandle h,
        List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
        final StatementInfo statementInfo = statementCache.getIfPresent(h.id);
        if (null == statementInfo) {
            throw new NoSuchStatementException(h);
        }
        String sql = h.signature.sql;
        for (TypedValue value : parameterValues) {
            if (value.type == Rep.BYTE || value.type == Rep.SHORT || value.type == Rep.LONG || value.type == Rep.DOUBLE
                || value.type == Rep.INTEGER || value.type == Rep.FLOAT) {
                sql = sql.replaceFirst("\\?", value.value.toString());
            } else {
                sql = sql.replaceFirst("\\?", "'" + value.value.toString() + "'");
            }
        }
        ExecuteResult executeResult = null;
        try {
            QuicksqlConnectionImpl connection = (QuicksqlConnectionImpl) getConnection(h.connectionId);
            String jdbcUrl = connection.getInfoByName("jdbcUrl");
            if (StringUtils.isNotBlank(jdbcUrl)) {
                executeResult = jdbcExecute(h, jdbcUrl, connection.getInfoByName("user"), connection
                    .getInfoByName("password"), sql);
            } else {
                executeResult = getExecuteResultSet(h, connection, sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return executeResult;
    }

    @SuppressWarnings("deprecation")
    public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
        long maxRowCount, PrepareCallback callback) {
        return prepareAndExecute(h, sql, maxRowCount, AvaticaUtils.toSaturatedInt(maxRowCount),
            callback);
    }

    public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount,
        int maxRowsInFirstFrame, PrepareCallback callback) {
        ExecuteResult executeResult = null;
        try {
            QuicksqlConnectionImpl connection = (QuicksqlConnectionImpl) getConnection(h.connectionId);
            String jdbcUrl = connection.getInfoByName("jdbcUrl");
            if (StringUtils.isNotBlank(jdbcUrl)) {
                executeResult = jdbcExecute(h, jdbcUrl, connection.getInfoByName("user"), connection
                    .getInfoByName("password"), sql);
            } else {
                executeResult = getExecuteResultSet(h, connection, sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return executeResult;
    }

    private ExecuteResult getExecuteResultSet(StatementHandle h, QuicksqlConnectionImpl connection, String sql) {
        MetaResultSet resultSet = null;
        String responseUrl = "";
        try {
            int maxResNum = Integer
                .parseInt(StringUtils.defaultIfBlank(connection.getInfoByName("acceptedResultsNum"), "100000"));
            responseUrl = connection.getInfoByName("responseUrl");
            System.out.println("sql:" + sql);
            System.out.println("responseUrl:" + responseUrl);
            System.out.println("schemaPath:" + connection.getInfoByName("schemaPath"));

            SqlRunner runner = SqlRunner.builder()
                .setTransformRunner(RunnerType.value(connection.getInfoByName("runner")))
                .setSchemaPath(StringUtils.isNotBlank(connection.getInfoByName("schemaPath")) ? "inline:" + connection
                    .getInfoByName("schemaPath") : SqlUtil.getSchemaPath(SqlUtil.parseTableName(sql).tableNames))
                .setAppName(StringUtils.defaultIfBlank(connection.getInfoByName("appName"), ""))
                .setAcceptedResultsNum(maxResNum)
                .ok();
            if (sql.contains("HDFS")) {
                insertResult(sql, runner, connection);
                resultSet = getResultSet(h, sql, 0, new QueryResult(new ArrayList<>(), new ArrayList<>()));
            } else {
                RunnerType runnerType = RunnerType.value(connection.getInfoByName("runner"));
                Object collect = runner.sql(sql).collect();
                switch (runnerType) {
                    case JDBC:
                        resultSet = getJDBCResultSet(h, collect, maxResNum);
                        break;
                    case SPARK:
                        Map.Entry<List<Attribute>, List<GenericRowWithSchema>> sparkData = (Entry<List<Attribute>, List<GenericRowWithSchema>>) collect;
                        resultSet = getResultSet(h, sql, maxResNum, getSparkQueryResult(sparkData));
                        break;
                    case FLINK:
                        Map.Entry<TableSchema, List<Row>> flinkData = (Entry<TableSchema, List<Row>>) collect;
                        resultSet = getResultSet(h, sql, maxResNum, getFlinkQueryResult(flinkData));
                        break;
                    case DEFAULT:
                        resultSet = getResultSet(h, runner, sql, maxResNum, collect);
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            setHttpResponse(responseUrl, 0, "Quicksql error :" + e.getCause());
            throw new RuntimeException(e);
        }
        return new ExecuteResult(Collections.singletonList(resultSet));
    }

    private ExecuteResult jdbcExecute(StatementHandle h, String jdbcUrl, String user, String password, String sql)
        throws SQLException, ClassNotFoundException {
        final StatementInfo info = getStatementCache().getIfPresent(h.id);
        Class.forName(matchDriver(jdbcUrl));
        Connection conn = null;
        Statement statement = null;
        List<MetaResultSet> resultSets = new ArrayList<>();
        try {
            conn = DriverManager.getConnection(jdbcUrl, user, password);
            conn.setAutoCommit(false);
            statement = conn.createStatement();
            statement.execute(sql);
            conn.commit();
            if (null == statement.getResultSet()) {
                resultSets.add(
                    QuicksqlServerResultSet.count(h.connectionId, h.id, AvaticaUtils.getLargeUpdateCount(statement)));
            } else {
                info.setResultSet(statement.getResultSet());
                resultSets.add(
                    QuicksqlServerResultSet.create(h.connectionId, h.id, statement.getResultSet(), 100));
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (conn != null) {
                conn.rollback();
            }
            throw new RuntimeException(e);
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
        return new ExecuteResult(resultSets);
    }


    private void insertResult(String sql, SqlRunner runner, QuicksqlConnectionImpl connection) {
        new Thread(() -> {
            int tag = 1;
            String message = "";
            try {
                runner.sql(sql).run();
            } catch (Throwable e) {
                e.printStackTrace();
                message = "Quicksql error :" + e.getMessage();
                tag = 0;
            }
            setHttpResponse(connection.getInfoByName("responseUrl"), tag, message);

        }).start();
    }

    private void setHttpResponse(String responseUrl, int tag, String message) {
        if (StringUtils.isNotBlank(responseUrl)) {
            JsonObject jsonObject = new JsonObject();
            try {
                String url = HttpUtils.parseUrlArgs(responseUrl, jsonObject);
                jsonObject.addProperty("message", message);
                jsonObject.addProperty("response", tag);
                System.out.println("response args:" + jsonObject.toString());
                String response = HttpUtils.post(url, jsonObject.toString());
                System.out.println(response);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private String matchDriver(String jdbcUrl) {
        Map<String, Map<String, String>> sourceMap = YmlUtils.getSourceMap();
        for (String driverName : sourceMap.keySet()) {
            if (jdbcUrl.contains(driverName)) {
                return sourceMap.get(driverName).get("driver");
            }
        }
        throw new RuntimeException("not match driver");
    }

    /**
     * Sets the provided maximum number of rows on the given statement.
     *
     * @param statement The JDBC Statement to operate on
     * @param maxRowCount The maximum number of rows which should be returned for the query
     */
    void setMaxRows(Statement statement, long maxRowCount) throws SQLException {
        // Special handling of maxRowCount as JDBC 0 is unlimited, our meta 0 row
        if (maxRowCount > 0) {
            AvaticaUtils.setLargeMaxRows(statement, maxRowCount);
        } else if (maxRowCount < 0) {
            statement.setMaxRows(0);
        }
    }

    public boolean syncResults(StatementHandle sh, QueryState state, long offset) {
        return true;
    }

    private QuicksqlServerResultSet getResultSet(StatementHandle h, SqlRunner runner, String sql, int maxResNum,
        Object collect) {
        if (collect instanceof ResultSet) {
            return getJDBCResultSet(h, collect, maxResNum);
        } else if (collect instanceof Map.Entry) {
            try {
                Map.Entry<List<Attribute>, List<GenericRowWithSchema>> sparkData = (Entry<List<Attribute>, List<GenericRowWithSchema>>) collect;
                return getResultSet(h, sql, maxResNum, getSparkQueryResult(sparkData));
            } catch (Exception e) {
                throw new RuntimeException("result type error " + e.getMessage());
            }
        } else {
            throw new RuntimeException("not matching result type");
        }
    }

    protected QuicksqlServerResultSet getResultSet(StatementHandle h, String sql, int maxResNum, QueryResult result)
        throws Exception {
        final List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.addAll(result.columnMeta);
        final StatementInfo info = getStatementCache().getIfPresent(h.id);
        Signature signature = preparedSignature(result, sql);
        Cursor cursor = MetaImpl.createCursor(signature.cursorFactory, result.iterable);
        QuicksqlResultSet quickSqlResultSet = new QuicksqlResultSet((AvaticaStatement) info.statement, signature,
            new AvaticaResultSetMetaData((AvaticaStatement) info.statement, null, signature), TimeZone.getDefault(),
            null);
        quickSqlResultSet.execute2(cursor, columnMetaDataList);
        info.setResultSet(quickSqlResultSet);
        return QuicksqlServerResultSet.create(h.connectionId, h.id, quickSqlResultSet, signature, maxResNum);
    }

    private QuicksqlServerResultSet getJDBCResultSet(StatementHandle h, Object collect, int maxResNum) {
        final StatementInfo info = getStatementCache().getIfPresent(h.id);
        if (info != null) {
            info.setResultSet((ResultSet)collect);
        }
        return QuicksqlServerResultSet.create(h.connectionId, h.id, (ResultSet) collect, maxResNum);
    }

    public QueryResult getSparkQueryResult(Map.Entry<List<Attribute>, List<GenericRowWithSchema>> sparkData)
        throws Exception {
        if (sparkData == null) {
            return new QueryResult(new ArrayList<>(), new ArrayList<>());
        }
        if (CollectionUtils.isEmpty(sparkData.getKey())) {
            throw new SparkException("collect data error");
        }
        List<Attribute> attributes = sparkData.getKey();
        List<GenericRowWithSchema> value = sparkData.getValue();
        List<Object> data = new ArrayList<>();
        List<ColumnMetaData> meta = new ArrayList<>();
        value.stream().forEach(column -> {
            data.add(column.values());
        });
        attributes.stream().forEach(attribute -> {
            ScalarType columnType = getColumnType(attribute.dataType());
            meta.add(new ColumnMetaData(0, false, true, false, false,
                1, true, -1, null, attribute.name(), null, -1, -1, null, null,
                columnType, true, false, false, columnType.columnClassName()));
        });
        return new QueryResult(meta, data);
    }

    public QueryResult getFlinkQueryResult(Map.Entry<TableSchema, List<Row>> sparkData) throws Exception {
        if (sparkData == null) {
            return new QueryResult(new ArrayList<>(), new ArrayList<>());
        }
        TableSchema tableSchema = sparkData.getKey();
        if (tableSchema == null || tableSchema.getFieldDataTypes().length != tableSchema.getFieldNames().length) {
            throw new SparkException("collect data error");
        }
        org.apache.flink.table.types.DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();
        String[] fieldNames = tableSchema.getFieldNames();
        List<Row> value = sparkData.getValue();
        List<Object> data = new ArrayList<>();
        List<ColumnMetaData> meta = new ArrayList<>();
        value.stream().forEach(column -> {
            Object[] objects = new Object[column.getArity()];
            for (int i = 0; i < column.getArity(); i++) {
                objects[i] = column.getField(i);
            }
            data.add(Arrays.asList(objects));
        });
        for (int i = 0; i < fieldNames.length; i++) {
            ScalarType columnType = getColumnType2(fieldDataTypes[i]);
            meta.add(new ColumnMetaData(0, false, true, false, false,
                1, true, -1, null, fieldNames[i], null, -1, -1, null, null,
                columnType, true, false, false, columnType.columnClassName()));
        }
        return new QueryResult(meta, data);
    }

    public Signature preparedSignature(QueryResult res, String sql) {
        List<AvaticaParameter> params = new ArrayList<AvaticaParameter>();
        int startIndex = 0;
        while (sql.indexOf("?", startIndex) >= 0) {
            AvaticaParameter param = new AvaticaParameter(false, 0, 0, 0, null, null, null);
            params.add(param);
            startIndex = sql.indexOf("?", startIndex) + 1;
        }
        return new Signature(res.columnMeta, sql, params, Collections.<String, Object>emptyMap(), CursorFactory.ARRAY,
            Meta.StatementType.SELECT);
    }

    private ScalarType getColumnType(DataType dataType) {
        if (dataType != null && StringUtils.isNotBlank(dataType.typeName())) {
            if (dataType.equals(DataTypes.StringType)) {
                return ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING);
            } else if (dataType.equals(DataTypes.BinaryType)) {
                return ColumnMetaData.scalar(Types.BINARY, "char", Rep.CHARACTER);
            } else if (dataType.equals(DataTypes.BooleanType)) {
                return ColumnMetaData.scalar(Types.BOOLEAN, "boolean", Rep.BOOLEAN);
            } else if (dataType.equals(DataTypes.DateType)) {
                return ColumnMetaData.scalar(Types.DATE, "date", Rep.JAVA_SQL_DATE);
            } else if (dataType.equals(DataTypes.TimestampType)) {
                return ColumnMetaData.scalar(Types.TIMESTAMP, "timestamp", Rep.JAVA_SQL_TIMESTAMP);
            } else if (dataType.equals(DataTypes.CalendarIntervalType)) {
                return ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING);
            } else if (dataType.equals(DataTypes.DoubleType)) {
                return ColumnMetaData.scalar(Types.DOUBLE, "double", Rep.DOUBLE);
            } else if (dataType.equals(DataTypes.FloatType)) {
                return ColumnMetaData.scalar(Types.FLOAT, "float", Rep.FLOAT);
            } else if (dataType.equals(DataTypes.ByteType)) {
                return ColumnMetaData.scalar(Types.TINYINT, "byte", Rep.BYTE);
            } else if (dataType.equals(DataTypes.IntegerType)) {
                return ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER);
            } else if (dataType.equals(DataTypes.LongType)) {
                return ColumnMetaData.scalar(Types.BIGINT, "long", Rep.LONG);
            } else if (dataType.equals(DataTypes.ShortType)) {
                return ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER);
            } else if (dataType.equals(DataTypes.NullType)) {
                return ColumnMetaData.scalar(Types.NULL, "null", Rep.OBJECT);
            }
        }
        return ColumnMetaData.scalar(Types.JAVA_OBJECT, "object", Rep.OBJECT);
    }

    private ScalarType getColumnType2(org.apache.flink.table.types.DataType dataType) {
        if (dataType != null && StringUtils.isNotBlank(dataType.toString())) {
            if (dataType.equals(DataTypes.StringType)) {
                return ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING);
            } else if (dataType.equals(DataTypes.BinaryType)) {
                return ColumnMetaData.scalar(Types.BINARY, "char", Rep.CHARACTER);
            } else if (dataType.equals(DataTypes.BooleanType)) {
                return ColumnMetaData.scalar(Types.BOOLEAN, "boolean", Rep.BOOLEAN);
            } else if (dataType.equals(DataTypes.DateType)) {
                return ColumnMetaData.scalar(Types.DATE, "date", Rep.JAVA_SQL_DATE);
            } else if (dataType.equals(DataTypes.TimestampType)) {
                return ColumnMetaData.scalar(Types.TIMESTAMP, "timestamp", Rep.JAVA_SQL_TIMESTAMP);
            } else if (dataType.equals(DataTypes.CalendarIntervalType)) {
                return ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING);
            } else if (dataType.equals(DataTypes.DoubleType)) {
                return ColumnMetaData.scalar(Types.DOUBLE, "double", Rep.DOUBLE);
            } else if (dataType.equals(DataTypes.FloatType)) {
                return ColumnMetaData.scalar(Types.FLOAT, "float", Rep.FLOAT);
            } else if (dataType.equals(DataTypes.ByteType)) {
                return ColumnMetaData.scalar(Types.TINYINT, "byte", Rep.BYTE);
            } else if (dataType.equals(DataTypes.IntegerType)) {
                return ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER);
            } else if (dataType.equals(DataTypes.LongType)) {
                return ColumnMetaData.scalar(Types.BIGINT, "long", Rep.LONG);
            } else if (dataType.equals(DataTypes.ShortType)) {
                return ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER);
            } else if (dataType.equals(DataTypes.NullType)) {
                return ColumnMetaData.scalar(Types.NULL, "null", Rep.OBJECT);
            }
        }
        return ColumnMetaData.scalar(Types.JAVA_OBJECT, "object", Rep.OBJECT);
    }

    public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws
        NoSuchStatementException, MissingResultsException {
        LOGGER.trace("fetching {} offset:{} fetchMaxRowCount:{}", h, offset, fetchMaxRowCount);
        try {
            final StatementInfo statementInfo = statementCache.getIfPresent(h.id);
            if (null == statementInfo) {
                // Statement might have expired, or never existed on this server.
                throw new NoSuchStatementException(h);
            }

            if (!statementInfo.isResultSetInitialized()) {
                // The Statement exists, but the results are missing. Need to call syncResults(...)
                throw new MissingResultsException(h);
            }
            if (statementInfo.getResultSet() == null) {
                return Frame.EMPTY;
            } else {
                return QuicksqlServerResultSet.frame(statementInfo, statementInfo.getResultSet(), offset,
                    fetchMaxRowCount, calendar, Optional.<Meta.Signature>absent());
            }
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    private static String[] toArray(List<String> typeList) {
        if (typeList == null) {
            return null;
        }
        return typeList.toArray(new String[typeList.size()]);
    }

    @Override
    public void commit(ConnectionHandle ch) {
        try {
            final Connection conn = getConnection(ch.id);
            conn.commit();
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public void rollback(ConnectionHandle ch) {
        try {
            final Connection conn = getConnection(ch.id);
            conn.rollback();
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h,
        List<String> sqlCommands) throws NoSuchStatementException {
        try {
            // Get the statement
            final StatementInfo info = statementCache.getIfPresent(h.id);
            if (info == null) {
                throw new NoSuchStatementException(h);
            }

            // addBatch() for each sql command
            final Statement stmt = info.statement;
            for (String sqlCommand : sqlCommands) {
                stmt.addBatch(sqlCommand);
            }

            // Execute the batch and return the results
            return new ExecuteBatchResult(AvaticaUtils.executeLargeBatch(stmt));
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public ExecuteBatchResult executeBatch(StatementHandle h,
        List<List<TypedValue>> updateBatches) throws NoSuchStatementException {
        try {
            final StatementInfo info = statementCache.getIfPresent(h.id);
            if (null == info) {
                throw new NoSuchStatementException(h);
            }

            final PreparedStatement preparedStmt = (PreparedStatement) info.statement;
            int rowUpdate = 1;
            for (List<TypedValue> batch : updateBatches) {
                int i = 1;
                for (TypedValue value : batch) {
                    // Set the TypedValue in the PreparedStatement
                    try {
                        preparedStmt.setObject(i, value.toJdbc(calendar));
                        i++;
                    } catch (SQLException e) {
                        throw new RuntimeException("Failed to set value on row #" + rowUpdate
                            + " and column #" + i, e);
                    }
                    // Track the update number for better error messages
                    rowUpdate++;
                }
                preparedStmt.addBatch();
            }
            return new ExecuteBatchResult(AvaticaUtils.executeLargeBatch(preparedStmt));
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public ExecuteBatchResult executeBatchProtobuf(StatementHandle h,
        List<Requests.UpdateBatch> updateBatches) throws NoSuchStatementException {
        try {
            final StatementInfo info = statementCache.getIfPresent(h.id);
            if (null == info) {
                throw new NoSuchStatementException(h);
            }

            final PreparedStatement preparedStmt = (PreparedStatement) info.statement;
            for (Requests.UpdateBatch update : updateBatches) {
                int i = 1;
                for (Common.TypedValue value : update.getParameterValuesList()) {
                    // Use the value and then increment
                    preparedStmt.setObject(i++, TypedValue.protoToJdbc(value, calendar));
                }
                preparedStmt.addBatch();
            }
            return new ExecuteBatchResult(AvaticaUtils.executeLargeBatch(preparedStmt));
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    /**
     * Configurable statement cache settings.
     */
    public enum StatementCacheSettings {
        /**
         * JDBC connection property for setting connection cache concurrency level.
         */
        CONCURRENCY_LEVEL(STMT_CACHE_KEY_BASE + ".concurrency", "100"),

        /**
         * JDBC connection property for setting connection cache initial capacity.
         */
        INITIAL_CAPACITY(STMT_CACHE_KEY_BASE + ".initialcapacity", "1000"),

        /**
         * JDBC connection property for setting connection cache maximum capacity.
         */
        MAX_CAPACITY(STMT_CACHE_KEY_BASE + ".maxcapacity", "10000"),

        /**
         * JDBC connection property for setting connection cache expiration duration.
         *
         * <p>Used in conjunction with {@link #EXPIRY_UNIT}.</p>
         */
        EXPIRY_DURATION(STMT_CACHE_KEY_BASE + ".expiryduration", "5"),

        /**
         * JDBC connection property for setting connection cache expiration unit.
         *
         * <p>Used in conjunction with {@link #EXPIRY_DURATION}.</p>
         */
        EXPIRY_UNIT(STMT_CACHE_KEY_BASE + ".expiryunit", TimeUnit.MINUTES.name());

        private final String key;
        private final String defaultValue;

        StatementCacheSettings(String key, String defaultValue) {
            this.key = key;
            this.defaultValue = defaultValue;
        }

        /**
         * The configuration key for specifying this setting.
         */
        public String key() {
            return key;
        }

        /**
         * The default value for this setting.
         */
        public String defaultValue() {
            return defaultValue;
        }
    }

    /**
     * Configurable connection cache settings.
     */
    public enum ConnectionCacheSettings {
        /**
         * JDBC connection property for setting connection cache concurrency level.
         */
        CONCURRENCY_LEVEL(CONN_CACHE_KEY_BASE + ".concurrency", "10"),

        /**
         * JDBC connection property for setting connection cache initial capacity.
         */
        INITIAL_CAPACITY(CONN_CACHE_KEY_BASE + ".initialcapacity", "100"),

        /**
         * JDBC connection property for setting connection cache maximum capacity.
         */
        MAX_CAPACITY(CONN_CACHE_KEY_BASE + ".maxcapacity", "1000"),

        /**
         * JDBC connection property for setting connection cache expiration duration.
         */
        EXPIRY_DURATION(CONN_CACHE_KEY_BASE + ".expiryduration", "10"),

        /**
         * JDBC connection property for setting connection cache expiration unit.
         */
        EXPIRY_UNIT(CONN_CACHE_KEY_BASE + ".expiryunit", TimeUnit.MINUTES.name());

        private final String key;
        private final String defaultValue;

        ConnectionCacheSettings(String key, String defaultValue) {
            this.key = key;
            this.defaultValue = defaultValue;
        }

        /**
         * The configuration key for specifying this setting.
         */
        public String key() {
            return key;
        }

        /**
         * The default value for this setting.
         */
        public String defaultValue() {
            return defaultValue;
        }
    }

    /**
     * Callback for {@link #connectionCache} member expiration.
     */
    private class ConnectionExpiryHandler
        implements RemovalListener<String, Connection> {

        public void onRemoval(RemovalNotification<String, Connection> notification) {
            String connectionId = notification.getKey();
            Connection doomed = notification.getValue();
            LOGGER.debug("Expiring connection {} because {}", connectionId, notification.getCause());
            try {
                if (doomed != null) {
                    doomed.close();
                }
            } catch (Throwable t) {
                LOGGER.info("Exception thrown while expiring connection {}", connectionId, t);
            }
        }
    }

    /**
     * Callback for {@link #statementCache} member expiration.
     */
    private class StatementExpiryHandler
        implements RemovalListener<Integer, StatementInfo> {

        public void onRemoval(RemovalNotification<Integer, StatementInfo> notification) {
            Integer stmtId = notification.getKey();
            StatementInfo doomed = notification.getValue();
            if (doomed == null) {
                // log/throw?
                return;
            }
            LOGGER.debug("Expiring statement {} because {}", stmtId, notification.getCause());
            try {
                if (doomed.getResultSet() != null) {
                    doomed.getResultSet().close();
                }
                if (doomed.statement != null) {
                    doomed.statement.close();
                }
            } catch (Throwable t) {
                LOGGER.info("Exception thrown while expiring statement {}", stmtId, t);
            }
        }
    }
}

// End QuicksqlServerMeta.java
