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
package com.qihoo.qsql.org.apache.calcite.test;

import com.qihoo.qsql.org.apache.calcite.adapter.clone.ArrayTableTest;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.TypeFinderTest;
import com.qihoo.qsql.org.apache.calcite.jdbc.CalciteRemoteDriverTest;
import com.qihoo.qsql.org.apache.calcite.materialize.LatticeSuggesterTest;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptPlanReaderTest;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptUtilTest;
import com.qihoo.qsql.org.apache.calcite.plan.RelTraitTest;
import com.qihoo.qsql.org.apache.calcite.plan.RelWriterTest;
import com.qihoo.qsql.org.apache.calcite.plan.volcano.*;
import com.qihoo.qsql.org.apache.calcite.prepare.LookupOperatorOverloadsTest;
import com.qihoo.qsql.org.apache.calcite.profile.ProfilerTest;
import com.qihoo.qsql.org.apache.calcite.rel.RelCollationTest;
import com.qihoo.qsql.org.apache.calcite.rel.RelDistributionTest;
import com.qihoo.qsql.org.apache.calcite.rel.rel2sql.RelToSqlConverterStructsTest;
import com.qihoo.qsql.org.apache.calcite.rel.rel2sql.RelToSqlConverterTest;
import com.qihoo.qsql.org.apache.calcite.rel.rules.DateRangeRulesTest;
import com.qihoo.qsql.org.apache.calcite.rel.rules.SortRemoveRuleTest;
import com.qihoo.qsql.org.apache.calcite.rex.RexBuilderTest;
import com.qihoo.qsql.org.apache.calcite.rex.RexExecutorTest;
import com.qihoo.qsql.org.apache.calcite.rex.RexSqlStandardConvertletTableTest;
import com.qihoo.qsql.org.apache.calcite.runtime.AutomatonTest;
import com.qihoo.qsql.org.apache.calcite.runtime.BinarySearchTest;
import com.qihoo.qsql.org.apache.calcite.runtime.EnumerablesTest;
import com.qihoo.qsql.org.apache.calcite.sql.SqlSetOptionOperatorTest;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParserTest;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlUnParserTest;
import com.qihoo.qsql.org.apache.calcite.sql.parser.parserextensiontesting.ExtensionSqlParserTest;
import com.qihoo.qsql.org.apache.calcite.sql.test.*;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlTypeFactoryTest;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlTypeUtilTest;
import com.qihoo.qsql.org.apache.calcite.sql.validate.LexCaseSensitiveTest;
import com.qihoo.qsql.org.apache.calcite.sql.validate.LexEscapeTest;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlValidatorUtilTest;
import com.qihoo.qsql.org.apache.calcite.test.enumerable.EnumerableCorrelateTest;
import com.qihoo.qsql.org.apache.calcite.test.enumerable.EnumerableRepeatUnionHierarchyTest;
import com.qihoo.qsql.org.apache.calcite.test.enumerable.EnumerableRepeatUnionTest;
import com.qihoo.qsql.org.apache.calcite.test.fuzzer.RexProgramFuzzyTest;
import com.qihoo.qsql.org.apache.calcite.tools.FrameworksTest;
import com.qihoo.qsql.org.apache.calcite.tools.PlannerTest;
import com.qihoo.qsql.org.apache.calcite.util.*;
import com.qihoo.qsql.org.apache.calcite.util.graph.DirectedGraphTest;
import com.qihoo.qsql.org.apache.calcite.util.mapping.MappingTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Calcite test suite.
 *
 * <p>Tests are sorted by approximate running time. The suite runs the fastest
 * tests first, so that regressions can be discovered as fast as possible.
 * Most unit tests run very quickly, and are scheduled before system tests
 * (which are slower but more likely to break because they have more
 * dependencies). Slow unit tests that don't break often are scheduled last.</p>
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // very fast tests (under 0.1s)
    ArrayTableTest.class,
    BitSetsTest.class,
    ImmutableBitSetTest.class,
    DirectedGraphTest.class,
    ReflectVisitorTest.class,
    RelOptUtilTest.class,
    RelCollationTest.class,
    UtilTest.class,
    PrecedenceClimbingParserTest.class,
    SourceTest.class,
    MappingTest.class,
    CalciteResourceTest.class,
    FilteratorTest.class,
    PermutationTestCase.class,
    SqlFunctionsTest.class,
    SqlJsonFunctionsTest.class,
    SqlTypeNameTest.class,
    ModelTest.class,
    TypeCoercionTest.class,
    TypeCoercionConverterTest.class,
    SqlValidatorFeatureTest.class,
    VolcanoPlannerTraitTest.class,
    InterpreterTest.class,
    TestUtilTest.class,
    VolcanoPlannerTest.class,
    RelTraitTest.class,
    HepPlannerTest.class,
    TraitPropagationTest.class,
    RelDistributionTest.class,
    RelWriterTest.class,
    RexProgramTest.class,
    SqlOperatorBindingTest.class,
    RexTransformerTest.class,
    AutomatonTest.class,
    BinarySearchTest.class,
    EnumerablesTest.class,
    ExceptionMessageTest.class,
    InduceGroupingTypeTest.class,
    RelOptPlanReaderTest.class,
    RexBuilderTest.class,
    RexSqlStandardConvertletTableTest.class,
    SqlTypeFactoryTest.class,
    SqlTypeUtilTest.class,
    SqlValidatorUtilTest.class,
    TypeFinderTest.class,
    RexShuttleTest.class,

    // medium tests (above 0.1s)
    SqlParserTest.class,
    SqlUnParserTest.class,
    ExtensionSqlParserTest.class,
    SqlSetOptionOperatorTest.class,
    SqlPrettyWriterTest.class,
    SqlValidatorTest.class,
    SqlValidatorDynamicTest.class,
    SqlValidatorMatchTest.class,
    SqlAdvisorTest.class,
    RelMetadataTest.class,
    DateRangeRulesTest.class,
    ScannableTableTest.class,
    RexExecutorTest.class,
    SqlLimitsTest.class,
    JdbcFrontLinqBackTest.class,
    RelToSqlConverterTest.class,
    RelToSqlConverterStructsTest.class,
    SqlOperatorTest.class,
    ChunkListTest.class,
    FrameworksTest.class,
    EnumerableCorrelateTest.class,
    LookupOperatorOverloadsTest.class,
    LexCaseSensitiveTest.class,
    LexEscapeTest.class,
    CollationConversionTest.class,
    TraitConversionTest.class,
    ComboRuleTest.class,
    MutableRelTest.class,

    // slow tests (above 1s)
    DocumentationTest.class,
    UdfTest.class,
    UdtTest.class,
    TableFunctionTest.class,
    PlannerTest.class,
    RelBuilderTest.class,
    PigRelBuilderTest.class,
    RexImplicationCheckerTest.class,
    JdbcAdapterTest.class,
    LinqFrontJdbcBackTest.class,
    JdbcFrontJdbcBackLinqMiddleTest.class,
    RexProgramFuzzyTest.class,
    SqlToRelConverterTest.class,
    ProfilerTest.class,
    SqlStatisticProviderTest.class,
    SqlAdvisorJdbcTest.class,
    CoreQuidemTest.class,
    CalciteRemoteDriverTest.class,
    StreamTest.class,
    SortRemoveRuleTest.class,

    // above 10sec
    JdbcFrontJdbcBackTest.class,
    EnumerableRepeatUnionTest.class,
    EnumerableRepeatUnionHierarchyTest.class,
    // above 20sec
    JdbcTest.class,
    CalciteSqlOperatorTest.class,
    ReflectiveSchemaTest.class,
    RelOptRulesTest.class,

    // test cases
    TableInRootSchemaTest.class,
    RelMdColumnOriginsTest.class,
    MultiJdbcSchemaJoinTest.class,
    SqlLineTest.class,
    CollectionTypeTest.class,

    // slow tests that don't break often
    SqlToRelConverterExtendedTest.class,
    PartiallyOrderedSetTest.class,

    // above 30sec
    LatticeSuggesterTest.class,
    MaterializationTest.class,

    // above 120sec
    LatticeTest.class,

    // system tests and benchmarks (very slow, but usually only run if
    // '-Dcalcite.test.slow' is specified)
    FoodmartTest.class
    })
public class CalciteSuite {
}

// End CalciteSuite.java
