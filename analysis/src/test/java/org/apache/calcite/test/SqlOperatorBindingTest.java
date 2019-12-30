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
package org.apache.calcite.test;

import com.qihoo.qsql.org.apache.calcite.adapter.java.JavaTypeFactory;
import com.qihoo.qsql.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeSystem;
import com.qihoo.qsql.org.apache.calcite.rex.RexBuilder;
import com.qihoo.qsql.org.apache.calcite.rex.RexNode;
import com.qihoo.qsql.org.apache.calcite.rex.RexProgram;
import com.qihoo.qsql.org.apache.calcite.rex.RexProgramBuilder;
import com.qihoo.qsql.org.apache.calcite.rex.RexUtil;
import com.qihoo.qsql.org.apache.calcite.sql.SqlDataTypeSpec;
import com.qihoo.qsql.org.apache.calcite.sql.SqlLiteral;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.SqlUtil;
import com.qihoo.qsql.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParserPos;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlTypeName;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertSame;

/**
 * Unit tests for {@link RexProgram} and
 * {@link RexProgramBuilder}.
 */
public class SqlOperatorBindingTest {
  private RexBuilder rexBuilder;
  private RelDataType integerDataType;
  private SqlDataTypeSpec integerType;

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a SqlOperatorBindingTest.
   */
  public SqlOperatorBindingTest() {
    super();
  }

  @Before
  public void setUp() {
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    integerDataType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    integerType = SqlTypeUtil.convertTypeToSpec(integerDataType);
    rexBuilder = new RexBuilder(typeFactory);
  }

  /** Tests {@link com.qihoo.qsql.org.apache.calcite.sql.SqlUtil#isLiteral(SqlNode, boolean)},
   * which was added to enhance Calcite's public API
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1219">[CALCITE-1219]
   * Add a method to SqlOperatorBinding to determine whether operand is a
   * literal</a>.
   */
  @Test public void testSqlNodeLiteral() {
    final SqlNode literal = SqlLiteral.createExactNumeric(
        "0",
        SqlParserPos.ZERO);
    final SqlNode castLiteral = SqlStdOperatorTable.CAST.createCall(
        SqlParserPos.ZERO,
        literal,
        integerType);
    final SqlNode castCastLiteral = SqlStdOperatorTable.CAST.createCall(
        SqlParserPos.ZERO,
        castLiteral,
        integerType);

    // SqlLiteral is considered as a Literal
    assertSame(true, SqlUtil.isLiteral(literal, true));
    // CAST(SqlLiteral as type) is considered as a Literal
    assertSame(true, SqlUtil.isLiteral(castLiteral, true));
    // CAST(CAST(SqlLiteral as type) as type) is NOT considered as a Literal
    assertSame(false, SqlUtil.isLiteral(castCastLiteral, true));
  }

  /** Tests {@link com.qihoo.qsql.org.apache.calcite.rex.RexUtil#isLiteral(RexNode, boolean)},
   * which was added to enhance Calcite's public API
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1219">[CALCITE-1219]
   * Add a method to SqlOperatorBinding to determine whether operand is a
   * literal</a>.
   */
  @Test public void testRexNodeLiteral() {
    final RexNode literal = rexBuilder.makeZeroLiteral(
        integerDataType);

    final RexNode castLiteral = rexBuilder.makeCall(
        integerDataType,
        SqlStdOperatorTable.CAST,
        Lists.newArrayList(literal));

    final RexNode castCastLiteral = rexBuilder.makeCall(
        integerDataType,
        SqlStdOperatorTable.CAST,
        Lists.newArrayList(castLiteral));

    // RexLiteral is considered as a Literal
    assertSame(true, RexUtil.isLiteral(literal, true));
    // CAST(RexLiteral as type) is considered as a Literal
    assertSame(true, RexUtil.isLiteral(castLiteral, true));
    // CAST(CAST(RexLiteral as type) as type) is NOT considered as a Literal
    assertSame(false, RexUtil.isLiteral(castCastLiteral, true));
  }
}

// End SqlOperatorBindingTest.java
