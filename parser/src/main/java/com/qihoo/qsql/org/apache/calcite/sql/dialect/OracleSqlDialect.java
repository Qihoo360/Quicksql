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
package com.qihoo.qsql.org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeSystem;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import com.qihoo.qsql.org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import com.qihoo.qsql.org.apache.calcite.sql.SqlCall;
import com.qihoo.qsql.org.apache.calcite.sql.SqlDataTypeSpec;
import com.qihoo.qsql.org.apache.calcite.sql.SqlDateLiteral;
import com.qihoo.qsql.org.apache.calcite.sql.SqlDialect;
import com.qihoo.qsql.org.apache.calcite.sql.SqlLiteral;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.SqlTimeLiteral;
import com.qihoo.qsql.org.apache.calcite.sql.SqlTimestampLiteral;
import com.qihoo.qsql.org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import com.qihoo.qsql.org.apache.calcite.sql.SqlUtil;
import com.qihoo.qsql.org.apache.calcite.sql.SqlWriter;
import com.qihoo.qsql.org.apache.calcite.sql.fun.SqlFloorFunction;
import com.qihoo.qsql.org.apache.calcite.sql.fun.SqlLibraryOperators;
import com.qihoo.qsql.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParserPos;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A <code>SqlDialect</code> implementation for the Oracle database.
 */
public class OracleSqlDialect extends SqlDialect {

  /** OracleDB type system. */
  private static final RelDataTypeSystem ORACLE_TYPE_SYSTEM =
      new RelDataTypeSystemImpl() {
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case VARCHAR:
            // Maximum size of 4000 bytes for varchar2.
            return 4000;
          default:
            return super.getMaxPrecision(typeName);
          }
        }
      };

  public static final SqlDialect DEFAULT =
      new OracleSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.ORACLE)
          .withIdentifierQuoteString("\"")
          .withDataTypeSystem(ORACLE_TYPE_SYSTEM));

  /** Creates an OracleSqlDialect. */
  public OracleSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean supportsDataType(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
      return false;
    default:
      return super.supportsDataType(type);
    }
  }

  @Override public SqlNode getCastSpec(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
    case SMALLINT:
      castSpec = "_NUMBER(5)";
      break;
    case INTEGER:
      castSpec = "_NUMBER(10)";
      break;
    case BIGINT:
      castSpec = "_NUMBER(19)";
      break;
    case DOUBLE:
      castSpec = "_DOUBLE PRECISION";
      break;
    default:
      return super.getCastSpec(type);
    }

    return new SqlDataTypeSpec(
        new SqlUserDefinedTypeNameSpec(castSpec, SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  @Override protected boolean allowsAs() {
    return false;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public void unparseDateTimeLiteral(SqlWriter writer,
      SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    if (literal instanceof SqlTimestampLiteral) {
      writer.literal("TO_TIMESTAMP('"
          + literal.toFormattedString() + "', 'YYYY-MM-DD HH24:MI:SS.FF')");
    } else if (literal instanceof SqlDateLiteral) {
      writer.literal("TO_DATE('"
          + literal.toFormattedString() + "', 'YYYY-MM-DD')");
    } else if (literal instanceof SqlTimeLiteral) {
      writer.literal("TO_TIME('"
          + literal.toFormattedString() + "', 'HH24:MI:SS.FF')");
    } else {
      super.unparseDateTimeLiteral(writer, literal, leftPrec, rightPrec);
    }
  }

  @Override public List<String> getSingleRowTableName() {
    return ImmutableList.of("DUAL");
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      SqlUtil.unparseFunctionSyntax(SqlLibraryOperators.SUBSTR, writer, call);
    } else {
      switch (call.getKind()) {
      case FLOOR:
        if (call.operandCount() != 2) {
          super.unparseCall(writer, call, leftPrec, rightPrec);
          return;
        }

        final SqlLiteral timeUnitNode = call.operand(1);
        final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

        SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
            timeUnitNode.getParserPosition());
        SqlFloorFunction.unparseDatetimeFunction(writer, call2, "TRUNC", true);
        break;

      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
    }
  }
}

// End OracleSqlDialect.java
