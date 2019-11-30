/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qihoo.qsql.server;

import com.qihoo.qsql.client.QuicksqlConnectionImpl;
import com.qihoo.qsql.client.QuicksqlJdbc41Factory;
import java.sql.DriverManager;
import java.sql.SQLException;
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
    try {
      DriverManager.registerDriver(new Driver());
    } catch (SQLException e) {
      e.printStackTrace();
    }
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
        "org-apache-quicksql-jdbc.properties",
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
