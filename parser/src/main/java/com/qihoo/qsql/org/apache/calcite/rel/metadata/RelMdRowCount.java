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
import com.qihoo.qsql.org.apache.calcite.rel.SingleRel;
import com.qihoo.qsql.org.apache.calcite.rel.core.Aggregate;
import com.qihoo.qsql.org.apache.calcite.rel.core.Calc;
import com.qihoo.qsql.org.apache.calcite.rel.core.Filter;
import com.qihoo.qsql.org.apache.calcite.rel.core.Intersect;
import com.qihoo.qsql.org.apache.calcite.rel.core.Join;
import com.qihoo.qsql.org.apache.calcite.rel.core.Minus;
import com.qihoo.qsql.org.apache.calcite.rel.core.Project;
import com.qihoo.qsql.org.apache.calcite.rel.core.Sort;
import com.qihoo.qsql.org.apache.calcite.rel.core.TableScan;
import com.qihoo.qsql.org.apache.calcite.rel.core.Union;
import com.qihoo.qsql.org.apache.calcite.rel.core.Values;
import com.qihoo.qsql.org.apache.calcite.rex.RexDynamicParam;
import com.qihoo.qsql.org.apache.calcite.rex.RexLiteral;
import com.qihoo.qsql.org.apache.calcite.util.Bug;
import com.qihoo.qsql.org.apache.calcite.util.BuiltInMethod;
import com.qihoo.qsql.org.apache.calcite.util.ImmutableBitSet;
import com.qihoo.qsql.org.apache.calcite.util.NumberUtil;
import com.qihoo.qsql.org.apache.calcite.util.Util;

/**
 * RelMdRowCount supplies a default implementation of
 * {@link RelMetadataQuery#getRowCount} for the standard logical algebra.
 */
public class RelMdRowCount
    implements MetadataHandler<BuiltInMetadata.RowCount> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.ROW_COUNT.method, new RelMdRowCount());

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.RowCount> getDef() {
    return BuiltInMetadata.RowCount.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.RowCount#getRowCount()},
   * invoked using reflection.
   *
   * @see com.qihoo.qsql.org.apache.calcite.rel.metadata.RelMetadataQuery#getRowCount(RelNode)
   */
  public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
    return rel.estimateRowCount(mq);
  }

  public Double getRowCount(RelSubset subset, RelMetadataQuery mq) {
    if (!Bug.CALCITE_1048_FIXED) {
      return mq.getRowCount(Util.first(subset.getBest(), subset.getOriginal()));
    }
    Double v = null;
    for (RelNode r : subset.getRels()) {
      try {
        v = NumberUtil.min(v, mq.getRowCount(r));
      } catch (CyclicMetadataException e) {
        // ignore this rel; there will be other, non-cyclic ones
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
    return Util.first(v, 1e6d); // if set is empty, estimate large
  }

  public Double getRowCount(Union rel, RelMetadataQuery mq) {
    double rowCount = 0.0;
    for (RelNode input : rel.getInputs()) {
      Double partialRowCount = mq.getRowCount(input);
      if (partialRowCount == null) {
        return null;
      }
      rowCount += partialRowCount;
    }
    return rowCount;
  }

  public Double getRowCount(Intersect rel, RelMetadataQuery mq) {
    Double rowCount = null;
    for (RelNode input : rel.getInputs()) {
      Double partialRowCount = mq.getRowCount(input);
      if (rowCount == null
          || partialRowCount != null && partialRowCount < rowCount) {
        rowCount = partialRowCount;
      }
    }
    return rowCount;
  }

  public Double getRowCount(Minus rel, RelMetadataQuery mq) {
    Double rowCount = null;
    for (RelNode input : rel.getInputs()) {
      Double partialRowCount = mq.getRowCount(input);
      if (rowCount == null
          || partialRowCount != null && partialRowCount < rowCount) {
        rowCount = partialRowCount;
      }
    }
    return rowCount;
  }

  public Double getRowCount(Filter rel, RelMetadataQuery mq) {
    return RelMdUtil.estimateFilteredRows(rel.getInput(), rel.getCondition(),
        mq);
  }

  public Double getRowCount(Calc rel, RelMetadataQuery mq) {
    return RelMdUtil.estimateFilteredRows(rel.getInput(), rel.getProgram(), mq);
  }

  public Double getRowCount(Project rel, RelMetadataQuery mq) {
    return mq.getRowCount(rel.getInput());
  }

  public Double getRowCount(Sort rel, RelMetadataQuery mq) {
    Double rowCount = mq.getRowCount(rel.getInput());
    if (rowCount == null) {
      return null;
    }
    if (rel.offset instanceof RexDynamicParam) {
      return rowCount;
    }
    final int offset = rel.offset == null ? 0 : RexLiteral.intValue(rel.offset);
    rowCount = Math.max(rowCount - offset, 0D);

    if (rel.fetch != null) {
      if (rel.fetch instanceof RexDynamicParam) {
        return rowCount;
      }
      final int limit = RexLiteral.intValue(rel.fetch);
      if (limit < rowCount) {
        return (double) limit;
      }
    }
    return rowCount;
  }

  public Double getRowCount(EnumerableLimit rel, RelMetadataQuery mq) {
    Double rowCount = mq.getRowCount(rel.getInput());
    if (rowCount == null) {
      return null;
    }
    if (rel.offset instanceof RexDynamicParam) {
      return rowCount;
    }
    final int offset = rel.offset == null ? 0 : RexLiteral.intValue(rel.offset);
    rowCount = Math.max(rowCount - offset, 0D);

    if (rel.fetch != null) {
      if (rel.fetch instanceof RexDynamicParam) {
        return rowCount;
      }
      final int limit = RexLiteral.intValue(rel.fetch);
      if (limit < rowCount) {
        return (double) limit;
      }
    }
    return rowCount;
  }

  // Covers Converter, Interpreter
  public Double getRowCount(SingleRel rel, RelMetadataQuery mq) {
    return mq.getRowCount(rel.getInput());
  }

  public Double getRowCount(Join rel, RelMetadataQuery mq) {
    return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
  }

  public Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
    ImmutableBitSet groupKey = rel.getGroupSet();

    // rowCount is the cardinality of the group by columns
    Double distinctRowCount =
        mq.getDistinctRowCount(rel.getInput(), groupKey, null);
    if (distinctRowCount == null) {
      distinctRowCount = mq.getRowCount(rel.getInput()) / 10;
    }

    // Grouping sets multiply
    distinctRowCount *= rel.getGroupSets().size();

    return distinctRowCount;
  }

  public Double getRowCount(TableScan rel, RelMetadataQuery mq) {
    return rel.estimateRowCount(mq);
  }

  public Double getRowCount(Values rel, RelMetadataQuery mq) {
    return rel.estimateRowCount(mq);
  }
}

// End RelMdRowCount.java
