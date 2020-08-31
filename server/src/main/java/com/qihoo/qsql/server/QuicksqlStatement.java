package com.qihoo.qsql.server;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;

/**
 * Implementation of {@link java.sql.Statement}
 * for the QuickSql engine.
 */
public abstract class QuicksqlStatement extends AvaticaStatement {
  /**
   * Creates a QuicksqlStatement.
   *
   * @param connection Connection
   * @param h Statement handle
   * @param resultSetType Result set type
   * @param resultSetConcurrency Result set concurrency
   * @param resultSetHoldability Result set holdability
   */
  QuicksqlStatement(QuicksqlConnectionImpl connection, Meta.StatementHandle h,
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    super(connection, h, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }
}

// End QuicksqlStatement.java
