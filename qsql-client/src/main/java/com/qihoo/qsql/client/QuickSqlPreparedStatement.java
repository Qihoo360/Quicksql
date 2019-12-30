package com.qihoo.qsql.client;

import java.sql.SQLException;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;

/**
 * Implementation of {@link java.sql.PreparedStatement}
 * for the QuickSql engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using
 * {@link org.apache.calcite.avatica.AvaticaFactory#newPreparedStatement}.
 */
abstract class QuickSqlPreparedStatement extends AvaticaPreparedStatement {
  /**
   * Creates a QuickSqlPreparedStatement.
   *
   * @param connection Connection
   * @param h Statement handle
   * @param signature Result of preparing statement
   * @param resultSetType Result set type
   * @param resultSetConcurrency Result set concurrency
   * @param resultSetHoldability Result set holdability
   * @throws SQLException if database error occurs
   */
  protected QuickSqlPreparedStatement(QuicksqlConnectionImpl connection,
      Meta.StatementHandle h, Meta.Signature signature, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    super(connection, h, signature, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  @Override public QuicksqlConnectionImpl getConnection() throws SQLException {
    return (QuicksqlConnectionImpl) super.getConnection();
  }
}

// End QuickSqlPreparedStatement.java
