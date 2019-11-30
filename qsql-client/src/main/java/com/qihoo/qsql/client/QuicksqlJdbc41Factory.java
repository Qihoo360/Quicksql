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
package com.qihoo.qsql.client;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

/**
 * Implementation of {@link AvaticaFactory}
 * for QuickSql and JDBC 4.1 (corresponds to JDK 1.7).
 */
@SuppressWarnings("UnusedDeclaration")
public class QuicksqlJdbc41Factory extends QuicksqlFactory {
  /** Creates a factory for JDBC version 4.1. */
  public QuicksqlJdbc41Factory() {
    this(4, 1);
  }

  /** Creates a JDBC factory with given major/minor version number. */
  protected QuicksqlJdbc41Factory(int major, int minor) {
    super(major, minor);
  }

  public QuickSqlJdbc41Connection newConnection(UnregisteredDriver driver,
      AvaticaFactory factory, String url, Properties info) {
    return new QuickSqlJdbc41Connection(driver, factory, url, info);
  }

  public QuickSqlJdbc41DatabaseMetaData newDatabaseMetaData(
      AvaticaConnection connection) {
    return new QuickSqlJdbc41DatabaseMetaData(
        (QuicksqlConnectionImpl) connection);
  }

  public QuickSqlJdbc41Statement newStatement(AvaticaConnection connection,
      Meta.StatementHandle h,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) {
    return new QuickSqlJdbc41Statement(
        (QuicksqlConnectionImpl) connection,
        h,
        resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  public AvaticaPreparedStatement newPreparedStatement(
      AvaticaConnection connection,
      Meta.StatementHandle h,
      Signature signature,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    return new QuickSqlJdbc41PreparedStatement(
        (QuicksqlConnectionImpl) connection, h,
         signature, resultSetType,
        resultSetConcurrency, resultSetHoldability);
  }

  public AvaticaResultSet newResultSet(AvaticaStatement statement, QueryState state,
      Signature signature, TimeZone timeZone, Meta.Frame firstFrame)
      throws SQLException {
    final ResultSetMetaData metaData =
        newResultSetMetaData(statement, signature);
    QuicksqlConnectionImpl connection = (QuicksqlConnectionImpl)statement.getConnection();
    return new QuicksqlResultSet(statement, signature, metaData, timeZone,
        firstFrame);
  }


  public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,
      Signature signature) {
    return new AvaticaResultSetMetaData(statement, null, signature);
  }

  /** Implementation of connection for JDBC 4.1. */
  private static class QuickSqlJdbc41Connection extends QuicksqlConnectionImpl {
    QuickSqlJdbc41Connection(UnregisteredDriver driver, AvaticaFactory factory, String url,
        Properties info) {
      super(driver, factory, url, info);
    }
  }

  /** Implementation of statement for JDBC 4.1. */
  private static class QuickSqlJdbc41Statement extends QuicksqlStatement {
    QuickSqlJdbc41Statement(QuicksqlConnectionImpl connection,
        Meta.StatementHandle h, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) {
      super(connection, h, resultSetType, resultSetConcurrency,
          resultSetHoldability);
    }
  }

  /** Implementation of prepared statement for JDBC 4.1. */
  private static class QuickSqlJdbc41PreparedStatement
      extends QuickSqlPreparedStatement {
    QuickSqlJdbc41PreparedStatement(QuicksqlConnectionImpl connection,
        Meta.StatementHandle h, Signature signature,
        int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
      super(connection, h, signature, resultSetType, resultSetConcurrency,
          resultSetHoldability);
    }
  }

  /** Implementation of database metadata for JDBC 4.1. */
  private static class QuickSqlJdbc41DatabaseMetaData
      extends AvaticaDatabaseMetaData {
    QuickSqlJdbc41DatabaseMetaData(QuicksqlConnectionImpl connection) {
      super(connection);
    }
  }
}

// End QuicksqlJdbc41Factory.java
