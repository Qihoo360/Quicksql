package com.qihoo.qsql.server;

import java.util.Properties;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.UnregisteredDriver;

/**
 * Extension of {@link AvaticaFactory}
 * for Quicksql.
 */
public abstract class QuicksqlFactory implements AvaticaFactory {
  protected final int major;
  protected final int minor;

  /** Creates a JDBC factory with given major/minor version number. */
  protected QuicksqlFactory(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  public int getJdbcMajorVersion() {
    return major;
  }

  public int getJdbcMinorVersion() {
    return minor;
  }


  /** Creates a connection with a root schema. */
  public abstract AvaticaConnection newConnection(UnregisteredDriver driver,
      AvaticaFactory factory, String url, Properties info);
}

// End QuicksqlFactory.java
