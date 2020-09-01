package com.qihoo.qsql.client;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.AvaticaHttpClientFactory;
import org.apache.calcite.avatica.remote.AvaticaRemoteConnectionProperty;
import org.apache.calcite.avatica.remote.KerberosConnection;
import org.apache.calcite.avatica.remote.MockJsonService;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.RemoteProtobufService;
import org.apache.calcite.avatica.remote.RemoteService;
import org.apache.calcite.avatica.remote.Service;

/**
 * Avatica Remote JDBC driver.
 */
public class Driver extends UnregisteredDriver {

  public static final String CONNECT_STRING_PREFIX = "jdbc:quicksql:";

  static {
    new Driver().register();
  }

  public Driver() {
    super();
  }

  /**
   * Defines the method of message serialization used by the Driver
   */
  public enum Serialization {
    JSON,
    PROTOBUF;
  }

  @Override protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        Driver.class,
        "quicksql-jdbc.properties",
        "Avatica Remote JDBC Driver",
        "unknown version",
        "Avatica",
        "unknown version");
  }

  @Override protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<ConnectionProperty>();
    Collections.addAll(list, BuiltInConnectionProperty.values());
    Collections.addAll(list, AvaticaRemoteConnectionProperty.values());
    return list;
  }

  @Override public Meta createMeta(AvaticaConnection connection) {
    final ConnectionConfig config = connection.config();

    // Perform the login and launch the renewal thread if necessary
    final KerberosConnection kerberosUtil = createKerberosUtility(config);
    if (null != kerberosUtil) {
      kerberosUtil.login();
      connection.setKerberosConnection(kerberosUtil);
    }

    // Create a single Service and set it on the Connection instance
    final Service service = createService(connection, config);
    connection.setService(service);
    return new QuicksqlRemoteMeta(connection, service);
  }

  KerberosConnection createKerberosUtility(ConnectionConfig config) {
    final String principal = config.kerberosPrincipal();
    if (null != principal) {
      return new KerberosConnection(principal, config.kerberosKeytab());
    }
    return null;
  }

  /**
   * Creates a {@link Service} with the given {@link AvaticaConnection} and configuration.
   *
   * @param connection The {@link AvaticaConnection} to use.
   * @param config Configuration properties
   * @return A Service implementation.
   */
  Service createService(AvaticaConnection connection, ConnectionConfig config) {
    final Service.Factory metaFactory = config.factory();
    final Service service;
    if (metaFactory != null) {
      service = metaFactory.create(connection);
    } else if (config.url() != null) {
      final AvaticaHttpClient httpClient = getHttpClient(connection, config);
      final Serialization serializationType = getSerialization(config);
      switch (serializationType) {
      case JSON:
        service = new RemoteService(httpClient);
        break;
      case PROTOBUF:
        service = new RemoteProtobufService(httpClient, new ProtobufTranslationImpl());
        break;
      default:
        throw new IllegalArgumentException("Unhandled serialization type: " + serializationType);
      }
    } else {
      service = new MockJsonService(Collections.<String, String>emptyMap());
    }
    return service;
  }

  /**
   * Creates the HTTP client that communicates with the Avatica server.
   *
   * @param connection The {@link AvaticaConnection}.
   * @param config The configuration.
   * @return An {@link AvaticaHttpClient} implementation.
   */
  AvaticaHttpClient getHttpClient(AvaticaConnection connection, ConnectionConfig config) {
    URL url;
    try {
      url = new URL(config.url());
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    AvaticaHttpClientFactory httpClientFactory = config.httpClientFactory();

    return httpClientFactory.getClient(url, config, connection.getKerberosConnection());
  }

  @Override public Connection connect(String url, Properties info)
      throws SQLException {
    AvaticaConnection conn = (AvaticaConnection) super.connect(url, info);
    if (conn == null) {
      // It's not an url for our driver
      return null;
    }
    if (info.getProperty("type","").equals("server")) {
        return conn;
    }

    Service service = conn.getService();

    // super.connect(...) should be creating a service and setting it in the AvaticaConnection
    assert null != service;

    service.apply(
        new Service.OpenConnectionRequest(conn.id, Service.OpenConnectionRequest.serializeProperties(info)));

    return conn;
  }

  Serialization getSerialization(ConnectionConfig config) {
    final String serializationStr = config.serialization();
    Serialization serializationType = Serialization.JSON;
    if (null != serializationStr) {
      try {
        serializationType =
            Serialization.valueOf(serializationStr.toUpperCase(Locale.ROOT));
      } catch (Exception e) {
        // Log a warning instead of failing harshly? Intentionally no loggers available?
        throw new RuntimeException(e);
      }
    }

    return serializationType;
  }
}

// End Driver.java
