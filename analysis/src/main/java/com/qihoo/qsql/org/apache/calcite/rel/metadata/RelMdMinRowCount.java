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
package com.qihoo.qsql.org.apache.calcite.rel.metadata;

import com.qihoo.qsql.org.apache.calcite.adapter.enumerable.EnumerableLimit;
import com.qihoo.qsql.org.apache.calcite.plan.volcano.RelSubset;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.core.Aggregate;
import com.qihoo.qsql.org.apache.calcite.rel.core.Filter;
import com.qihoo.qsql.org.apache.calcite.rel.core.Intersect;
import com.qihoo.qsql.org.apache.calcite.rel.core.Join;
import com.qihoo.qsql.org.apache.calcite.rel.core.Minus;
import com.qihoo.qsql.org.apache.calcite.rel.core.Project;
import com.qihoo.qsql.org.apache.calcite.rel.core.Sort;
import com.qihoo.qsql.org.apache.calcite.rel.core.TableScan;
import com.qihoo.qsql.org.apache.calcite.rel.core.Union;
import com.qihoo.qsql.org.apache.calcite.rel.core.Values;
import com.qihoo.qsql.org.apache.calcite.rex.RexLiteral;
import com.qihoo.qsql.org.apache.calcite.util.Bug;
import com.qihoo.qsql.org.apache.calcite.util.BuiltInMethod;
import com.qihoo.qsql.org.apache.calcite.util.Util;

/**
 * RelMdMinRowCount supplies a default implementation of
 * {@link RelMetadataQuery#getMinRowCount} for the standard logical algebra.
 */
public class RelMdMinRowCount
    implements MetadataHandler<BuiltInMetadata.MinRowCount> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.MIN_ROW_COUNT.method, new RelMdMinRowCount());

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.MinRowCount> getDef() {
    return BuiltInMetadata.MinRowCount.DEF;
  }

  public Double getMinRowCount(Union rel, RelMetadataQuery mq) {
    double rowCount = 0.0;
    for (RelNode input : rel.getInputs()) {
      Double partialRowCount = mq.getMinRowCount(input);
      if (partialRowCount != null) {
        rowCount += partialRowCount;
      }
    }
    return rowCount;
  }

  public Double getMinRowCount(Intersect rel, RelMetadataQuery mq) {
    return 0d; // no lower bound
  }

  public Double getMinRowCount(Minus rel, RelMetadataQuery mq) {
    return 0d; // no lower bound
  }

  public Double getMinRowCount(Filter rel, RelMetadataQuery mq) {
    return 0d; // no lower bound
  }

  public Double getMinRowCount(Project rel, RelMetadataQuery mq) {
    return mq.getMinRowCount(rel.getInput());
  }

  public Double getMinRowCount(Sort rel, RelMetadataQuery mq) {
    Double rowCount = mq.getMinRowCount(rel.getInput());
    if (rowCount == null) {
      rowCount = 0D;
    }
    final int offset = rel.offset == null ? 0 : RexLiteral.intValue(rel.offset);
    rowCount = Math.max(rowCount - offset, 0D);

    if (rel.fetch != null) {
      final int limit = RexLiteral.intValue(rel.fetch);
      if (limit < rowCount) {
        return (double) limit;
      }
    }
    return rowCount;
  }

  public Double getMinRowCount(EnumerableLimit rel, RelMetadataQuery mq) {
    Double rowCount = mq.getMinRowCount(rel.getInput());
    if (rowCount == null) {
      rowCount = 0D;
    }
    final int offset = rel.offset == null ? 0 : RexLiteral.intValue(rel.offset);
    rowCount = Math.max(rowCount - offset, 0D);

    if (rel.fetch != null) {
      final int limit = RexLiteral.intValue(rel.fetch);
      if (limit < rowCount) {
        return (double) limit;
      }
    }
    return rowCount;
  }

  public Double getMinRowCount(Aggregate rel, RelMetadataQuery mq) {
    if (rel.getGroupSet().isEmpty()) {
      // Aggregate with no GROUP BY always returns 1 row (even on empty table).
      return 1D;
    }
    final Double rowCount = mq.getMinRowCount(rel.getInput());
    if (rowCount != null && rowCount >= 1D) {
      return (double) rel.getGroupSets().size();
    }
    return 0D;
  }

  public Double getMinRowCount(Join rel, RelMetadataQuery mq) {
    return 0D;
  }

  public Double getMinRowCount(TableScan rel, RelMetadataQuery mq) {
    return 0D;
  }

  public Double getMinRowCount(Values values, RelMetadataQuery mq) {
    // For Values, the minimum row count is the actual row count.
    return (double) values.getTuples().size();
  }

  public Double getMinRowCount(RelSubset rel, RelMetadataQuery mq) {
    // FIXME This is a short-term fix for [CALCITE-1018]. A complete
    // solution will come with [CALCITE-1048].
    Util.discard(Bug.CALCITE_1048_FIXED);
    for (RelNode node : rel.getRels()) {
      if (node instanceof Sort) {
        Sort sort = (Sort) node;
        if (sort.fetch != null) {
          return (double) RexLiteral.intValue(sort.fetch);
        }
      }
    }

    return 0D;
  }

  // Catch-all rule when none of the others apply.
  public Double getMinRowCount(RelNode rel, RelMetadataQuery mq) {
    return null;
  }
}

// End RelMdMinRowCount.java
