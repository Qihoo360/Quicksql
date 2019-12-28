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
package com.qihoo.qsql.org.apache.calcite.sql.validate;

import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.EnumerableConvention;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.EnumerableProject;
import com.qihoo.qsql.org.apache.calcite.config.Lex;
import com.qihoo.qsql.org.apache.calcite.plan.RelTraitDef;
import com.qihoo.qsql.org.apache.calcite.plan.RelTraitSet;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParseException;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParser;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.test.CalciteAssert;
import com.qihoo.qsql.org.apache.calcite.tools.FrameworkConfig;
import com.qihoo.qsql.org.apache.calcite.tools.Frameworks;
import com.qihoo.qsql.org.apache.calcite.tools.Planner;
import com.qihoo.qsql.org.apache.calcite.tools.Program;
import com.qihoo.qsql.org.apache.calcite.tools.Programs;
import com.qihoo.qsql.org.apache.calcite.tools.RelConversionException;
import com.qihoo.qsql.org.apache.calcite.tools.ValidationException;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Testing {@link SqlValidator} and {@link Lex}.
 */
public class LexCaseSensitiveTest {

  private static Planner getPlanner(List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig, Program... programs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR))
        .traitDefs(traitDefs)
        .programs(programs)
        .build();
    return Frameworks.getPlanner(config);
  }

  private static void runProjectQueryWithLex(Lex lex, String sql)
      throws SqlParseException, ValidationException, RelConversionException {
    Config javaLex = SqlParser.configBuilder().setLex(lex).build();
    Planner planner = getPlanner(null, javaLex, Programs.ofRules(Programs.RULE_SET));
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    RelTraitSet traitSet =
        planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    assertThat(transform, instanceOf(EnumerableProject.class));
    List<String> fieldNames = transform.getRowType().getFieldNames();
    assertThat(fieldNames.size(), is(2));
    if (lex.caseSensitive) {
      assertThat(fieldNames.get(0), is("EMPID"));
      assertThat(fieldNames.get(1), is("empid"));
    } else {
      assertThat(fieldNames.get(0) + "-" + fieldNames.get(1),
          anyOf(is("EMPID-empid0"), is("EMPID0-empid")));
    }
  }

  @Test public void testCalciteCaseOracle()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select \"empid\" as EMPID, \"empid\" from\n"
        + " (select \"empid\" from \"emps\" order by \"emps\".\"deptno\")";
    runProjectQueryWithLex(Lex.ORACLE, sql);
  }

  @Test(expected = ValidationException.class)
  public void testCalciteCaseOracleException()
      throws SqlParseException, ValidationException, RelConversionException {
    // Oracle is case sensitive, so EMPID should not be found.
    String sql = "select EMPID, \"empid\" from\n"
        + " (select \"empid\" from \"emps\" order by \"emps\".\"deptno\")";
    runProjectQueryWithLex(Lex.ORACLE, sql);
  }

  @Test public void testCalciteCaseMySql()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select empid as EMPID, empid from (\n"
        + "  select empid from emps order by `EMPS`.DEPTNO)";
    runProjectQueryWithLex(Lex.MYSQL, sql);
  }

  @Test public void testCalciteCaseMySqlNoException()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select EMPID, empid from\n"
        + " (select empid from emps order by emps.deptno)";
    runProjectQueryWithLex(Lex.MYSQL, sql);
  }

  @Test public void testCalciteCaseMySqlAnsi()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select empid as EMPID, empid from (\n"
        + "  select empid from emps order by EMPS.DEPTNO)";
    runProjectQueryWithLex(Lex.MYSQL_ANSI, sql);
  }

  @Test public void testCalciteCaseMySqlAnsiNoException()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select EMPID, empid from\n"
        + " (select empid from emps order by emps.deptno)";
    runProjectQueryWithLex(Lex.MYSQL_ANSI, sql);
  }

  @Test public void testCalciteCaseSqlServer()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select empid as EMPID, empid from (\n"
        + "  select empid from emps order by EMPS.DEPTNO)";
    runProjectQueryWithLex(Lex.SQL_SERVER, sql);
  }

  @Test public void testCalciteCaseSqlServerNoException()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select EMPID, empid from\n"
        + " (select empid from emps order by emps.deptno)";
    runProjectQueryWithLex(Lex.SQL_SERVER, sql);
  }

  @Test public void testCalciteCaseJava()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select empid as EMPID, empid from (\n"
        + "  select empid from emps order by emps.deptno)";
    runProjectQueryWithLex(Lex.JAVA, sql);
  }

  @Test(expected = ValidationException.class)
  public void testCalciteCaseJavaException()
      throws SqlParseException, ValidationException, RelConversionException {
    // JAVA is case sensitive, so EMPID should not be found.
    String sql = "select EMPID, empid from\n"
        + " (select empid from emps order by emps.deptno)";
    runProjectQueryWithLex(Lex.JAVA, sql);
  }

  @Test public void testCalciteCaseJoinOracle()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select t.\"empid\" as EMPID, s.\"empid\" from\n"
        + "(select * from \"emps\" where \"emps\".\"deptno\" > 100) t join\n"
        + "(select * from \"emps\" where \"emps\".\"deptno\" < 200) s\n"
        + "on t.\"empid\" = s.\"empid\"";
    runProjectQueryWithLex(Lex.ORACLE, sql);
  }

  @Test public void testCalciteCaseJoinMySql()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select t.empid as EMPID, s.empid from\n"
        + "(select * from emps where emps.deptno > 100) t join\n"
        + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
    runProjectQueryWithLex(Lex.MYSQL, sql);
  }

  @Test public void testCalciteCaseJoinMySqlAnsi()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select t.empid as EMPID, s.empid from\n"
        + "(select * from emps where emps.deptno > 100) t join\n"
        + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
    runProjectQueryWithLex(Lex.MYSQL_ANSI, sql);
  }

  @Test public void testCalciteCaseJoinSqlServer()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select t.empid as EMPID, s.empid from\n"
        + "(select * from emps where emps.deptno > 100) t join\n"
        + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
    runProjectQueryWithLex(Lex.SQL_SERVER, sql);
  }

  @Test public void testCalciteCaseJoinJava()
      throws SqlParseException, ValidationException, RelConversionException {
    String sql = "select t.empid as EMPID, s.empid from\n"
        + "(select * from emps where emps.deptno > 100) t join\n"
        + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
    runProjectQueryWithLex(Lex.JAVA, sql);
  }
}

// End LexCaseSensitiveTest.java
