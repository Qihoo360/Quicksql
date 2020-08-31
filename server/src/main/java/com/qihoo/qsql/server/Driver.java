package com.qihoo.qsql.server;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;


/**
 * Calcite JDBC driver.
 */
public class Driver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:quicksql:server:";

  static {
      new Driver().register();
  }

  @Override protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
    case JDBC_30:
    case JDBC_40:
      throw new IllegalArgumentException("JDBC version not supported: "
          + jdbcVersion);
    case JDBC_41:
    default:
      return QuicksqlJdbc41Factory.class.getName();
    }
  }

  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        Driver.class,
        "quicksql-jdbc.properties",
        "QuickSql JDBC Driver",
        "unknown version",
        "QuickSql",
        "unknown version");
  }
  @Override public Meta createMeta(AvaticaConnection connection) {
    return new QuicksqlMetaImpl((QuicksqlConnectionImpl) connection);
  }

}

// End Driver.java
