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

import com.qihoo.qsql.org.apache.calcite.plan.RelOptCluster;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptRule;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptRuleCall;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptRuleOperand;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptUtil;
import com.qihoo.qsql.org.apache.calcite.rel.RelCollation;
import com.qihoo.qsql.org.apache.calcite.rel.RelCollationTraitDef;
import com.qihoo.qsql.org.apache.calcite.rel.RelCollations;
import com.qihoo.qsql.org.apache.calcite.rel.RelFieldCollation;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.core.Project;
import com.qihoo.qsql.org.apache.calcite.rel.core.RelFactories;
import com.qihoo.qsql.org.apache.calcite.rel.core.Sort;
import com.qihoo.qsql.org.apache.calcite.rel.logical.LogicalProject;
import com.qihoo.qsql.org.apache.calcite.rex.RexCall;
import com.qihoo.qsql.org.apache.calcite.rex.RexCallBinding;
import com.qihoo.qsql.org.apache.calcite.rex.RexNode;
import com.qihoo.qsql.org.apache.calcite.rex.RexUtil;
import com.qihoo.qsql.org.apache.calcite.sql.SqlKind;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlMonotonicity;
import com.qihoo.qsql.org.apache.calcite.tools.RelBuilderFactory;
import com.qihoo.qsql.org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Planner rule that pushes
 * a {@link com.qihoo.qsql.org.apache.calcite.rel.core.Sort}
 * past a {@link com.qihoo.qsql.org.apache.calcite.rel.core.Project}.
 *
 * @see com.qihoo.qsql.org.apache.calcite.rel.rules.ProjectSortTransposeRule
 */
public class SortProjectTransposeRule extends RelOptRule {
  public static final SortProjectTransposeRule INSTANCE =
      new SortProjectTransposeRule(Sort.class, LogicalProject.class,
          RelFactories.LOGICAL_BUILDER, null);

  //~ Constructors -----------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public SortProjectTransposeRule(
      Class<? extends Sort> sortClass,
      Class<? extends Project> projectClass) {
    this(sortClass, projectClass, RelFactories.LOGICAL_BUILDER, null);
  }

  @Deprecated // to be removed before 2.0
  public SortProjectTransposeRule(
      Class<? extends Sort> sortClass,
      Class<? extends Project> projectClass,
      String description) {
    this(sortClass, projectClass, RelFactories.LOGICAL_BUILDER, description);
  }

  /** Creates a SortProjectTransposeRule. */
  public SortProjectTransposeRule(
      Class<? extends Sort> sortClass,
      Class<? extends Project> projectClass,
      RelBuilderFactory relBuilderFactory, String description) {
    this(
        operand(sortClass,
            operand(projectClass, any())),
        relBuilderFactory, description);
  }

  /** Creates a SortProjectTransposeRule with an operand. */
  protected SortProjectTransposeRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description) {
    super(operand, relBuilderFactory, description);
  }

  @Deprecated // to be removed before 2.0
  protected SortProjectTransposeRule(RelOptRuleOperand operand) {
    super(operand);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Project project = call.rel(1);
    final RelOptCluster cluster = project.getCluster();

    if (sort.getConvention() != project.getConvention()) {
      return;
    }

    // Determine mapping between project input and output fields. If sort
    // relies on non-trivial expressions, we can't push.
    final Mappings.TargetMapping map =
        RelOptUtil.permutationIgnoreCast(
            project.getProjects(), project.getInput().getRowType());
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
        return;
      }
      final RexNode node = project.getProjects().get(fc.getFieldIndex());
      if (node.isA(SqlKind.CAST)) {
        // Check whether it is a monotonic preserving cast, otherwise we cannot push
        final RexCall cast = (RexCall) node;
        final RexCallBinding binding =
            RexCallBinding.create(cluster.getTypeFactory(), cast,
                ImmutableList.of(RelCollations.of(RexUtil.apply(map, fc))));
        if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
          return;
        }
      }
    }
    final RelCollation newCollation =
        cluster.traitSet().canonize(
            RexUtil.apply(map, sort.getCollation()));
    final Sort newSort =
        sort.copy(
            sort.getTraitSet().replace(newCollation),
            project.getInput(),
            newCollation,
            sort.offset,
            sort.fetch);
    RelNode newProject =
        project.copy(
            sort.getTraitSet(),
            ImmutableList.of(newSort));
    // Not only is newProject equivalent to sort;
    // newSort is equivalent to project's input
    // (but only if the sort is not also applying an offset/limit).
    Map<RelNode, RelNode> equiv;
    if (sort.offset == null
        && sort.fetch == null
        && cluster.getPlanner().getRelTraitDefs()
            .contains(RelCollationTraitDef.INSTANCE)) {
      equiv = ImmutableMap.of((RelNode) newSort, project.getInput());
    } else {
      equiv = ImmutableMap.of();
    }
    call.transformTo(newProject, equiv);
  }
}

// End SortProjectTransposeRule.java
