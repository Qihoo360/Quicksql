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
package com.qihoo.qsql.org.apache.calcite.rel.rules;

import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.EnumerableConvention;
import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.EnumerableRules;
import com.qihoo.qsql.org.apache.calcite.plan.ConventionTraitDef;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptUtil;
import com.qihoo.qsql.org.apache.calcite.plan.RelTraitSet;
import com.qihoo.qsql.org.apache.calcite.rel.RelCollationTraitDef;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.RelRoot;
import com.qihoo.qsql.org.apache.calcite.schemas.HrClusteredSchema;
import com.qihoo.qsql.org.apache.calcite.sql.SqlExplainFormat;
import com.qihoo.qsql.org.apache.calcite.sql.SqlExplainLevel;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParser;
import com.qihoo.qsql.org.apache.calcite.tools.*;
import com.qihoo.qsql.org.apache.calcite.util.Util;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests the application of the {@link SortRemoveRule}.
 */
public final class SortRemoveRuleTest {

  /**
   * The default schema that is used in these tests provides tables sorted on the primary key. Due
   * to this scan operators always come with a {@link com.qihoo.qsql.org.apache.calcite.rel.RelCollation} trait.
   */
  private RelNode transform(String sql, RuleSet prepareRules) throws Exception {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus defSchema = rootSchema.add("hr", new HrClusteredSchema());
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(defSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(
            Programs.of(prepareRules),
            Programs.ofRules(SortRemoveRule.INSTANCE))
        .build();
    Planner planner = Frameworks.getPlanner(config);
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelRoot planRoot = planner.rel(validate);
    RelNode planBefore = planRoot.rel;
    RelTraitSet desiredTraits = planBefore.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelNode planAfter = planner.transform(0, desiredTraits, planBefore);
    return planner.transform(1, desiredTraits, planAfter);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2554">[CALCITE-2554]
   * Enrich enumerable join operators with order preserving information</a>.
   *
   * <p>Since join inputs are sorted, and this join preserves the order of the
   * left input, there shouldn't be any sort operator above the join.
   */
  @Test public void removeSortOverEnumerableHashJoin() throws Exception {
    RuleSet prepareRules =
        RuleSets.ofList(
            SortProjectTransposeRule.INSTANCE,
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    for (String joinType : Arrays.asList("left", "right", "full", "inner")) {
      String sql =
          "select e.\"deptno\" from \"hr\".\"emps\" e "
              + joinType + " join \"hr\".\"depts\" d "
              + " on e.\"deptno\" = d.\"deptno\" "
              + "order by e.\"empid\" ";
      RelNode actualPlan = transform(sql, prepareRules);
      assertThat(
          toString(actualPlan),
          allOf(
              containsString("EnumerableHashJoin"),
              not(containsString("EnumerableSort"))));
    }
  }


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2554">[CALCITE-2554]
   * Enrich enumerable join operators with order preserving information</a>.
   *
   * <p>Since join inputs are sorted, and this join preserves the order of the
   * left input, there shouldn't be any sort operator above the join.
   */
  @Test public void removeSortOverEnumerableNestedLoopJoin() throws Exception {
    RuleSet prepareRules =
        RuleSets.ofList(
            SortProjectTransposeRule.INSTANCE,
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    // Inner join is not considered since the ENUMERABLE_JOIN_RULE does not generate a nestedLoop
    // join in the case of inner joins.
    for (String joinType : Arrays.asList("left", "right", "full")) {
      String sql =
          "select e.\"deptno\" from \"hr\".\"emps\" e "
              + joinType + " join \"hr\".\"depts\" d "
              + " on e.\"deptno\" > d.\"deptno\" "
              + "order by e.\"empid\" ";
      RelNode actualPlan = transform(sql, prepareRules);
      assertThat(
          toString(actualPlan),
          allOf(
              containsString("EnumerableNestedLoopJoin"),
              not(containsString("EnumerableSort"))));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2554">[CALCITE-2554]
   * Enrich enumerable join operators with order preserving information</a>.
   *
   * <p>Since join inputs are sorted, and this join preserves the order of the
   * left input, there shouldn't be any sort operator above the join.
   *
   * <p>Until CALCITE-2018 is fixed we can add back EnumerableRules.ENUMERABLE_SORT_RULE
   */
  @Test public void removeSortOverEnumerableCorrelate() throws Exception {
    RuleSet prepareRules =
        RuleSets.ofList(
            SortProjectTransposeRule.INSTANCE,
            JoinToCorrelateRule.INSTANCE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_CORRELATE_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    for (String joinType : Arrays.asList("left", "inner")) {
      String sql =
          "select e.\"deptno\" from \"hr\".\"emps\" e "
              + joinType + " join \"hr\".\"depts\" d "
              + " on e.\"deptno\" = d.\"deptno\" "
              + "order by e.\"empid\" ";
      RelNode actualPlan = transform(sql, prepareRules);
      assertThat(
          toString(actualPlan),
          allOf(
              containsString("EnumerableCorrelate"),
              not(containsString("EnumerableSort"))));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2554">[CALCITE-2554]
   * Enrich enumerable join operators with order preserving information</a>.
   *
   * <p>Since join inputs are sorted, and this join preserves the order of the
   * left input, there shouldn't be any sort operator above the join.
   */
  @Test public void removeSortOverEnumerableSemiJoin() throws Exception {
    RuleSet prepareRules =
        RuleSets.ofList(
            SortProjectTransposeRule.INSTANCE,
            SemiJoinRule.PROJECT,
            SemiJoinRule.JOIN,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    String sql =
        "select e.\"deptno\" from \"hr\".\"emps\" e\n"
            + " where e.\"deptno\" in (select d.\"deptno\" from \"hr\".\"depts\" d)\n"
            + " order by e.\"empid\"";
    RelNode actualPlan = transform(sql, prepareRules);
    assertThat(
        toString(actualPlan),
        allOf(
            containsString("EnumerableHashJoin"),
            not(containsString("EnumerableSort"))));
  }

  private String toString(RelNode rel) {
    return Util.toLinux(
        RelOptUtil.dumpPlan("", rel, SqlExplainFormat.TEXT,
            SqlExplainLevel.DIGEST_ATTRIBUTES));
  }
}

// End SortRemoveRuleTest.java
