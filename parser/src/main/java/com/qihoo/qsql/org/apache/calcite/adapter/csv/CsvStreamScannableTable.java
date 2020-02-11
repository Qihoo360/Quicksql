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
package com.qihoo.qsql.org.apache.calcite.adapter.csv;

import com.qihoo.qsql.org.apache.calcite.DataContext;
import com.qihoo.qsql.org.apache.calcite.adapter.java.JavaTypeFactory;
import com.qihoo.qsql.org.apache.calcite.linq4j.AbstractEnumerable;
import com.qihoo.qsql.org.apache.calcite.linq4j.Enumerable;
import com.qihoo.qsql.org.apache.calcite.linq4j.Enumerator;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeFactory;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelProtoDataType;
import com.qihoo.qsql.org.apache.calcite.schema.ScannableTable;
import com.qihoo.qsql.org.apache.calcite.schema.StreamableTable;
import com.qihoo.qsql.org.apache.calcite.schema.Table;
import com.qihoo.qsql.org.apache.calcite.util.Source;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table based on a CSV file.
 *
 * <p>It implements the {@link ScannableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext)} method.
 */
public class CsvStreamScannableTable extends CsvScannableTable
    implements StreamableTable {
  /** Creates a CsvScannableTable. */
  CsvStreamScannableTable(Source source, RelProtoDataType protoRowType) {
    super(source, protoRowType);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<>();
      return CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source, fieldTypes, true);
    } else {
      return CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source, null, true);
    }
  }

  public String toString() {
    return "CsvStreamScannableTable";
  }

  public Enumerable<Object[]> scan(DataContext root) {
    final int[] fields = CsvEnumerator.identityList(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new CsvEnumerator<>(source, cancelFlag, true, null,
            new CsvEnumerator.ArrayRowConverter(fieldTypes, fields, true));
      }
    };
  }

  @Override public Table stream() {
    return this;
  }
}

// End CsvStreamScannableTable.java
