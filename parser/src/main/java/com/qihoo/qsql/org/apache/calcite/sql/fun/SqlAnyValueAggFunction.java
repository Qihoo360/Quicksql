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
package com.qihoo.qsql.org.apache.calcite.sql.fun;

import com.qihoo.qsql.org.apache.calcite.sql.SqlAggFunction;
import com.qihoo.qsql.org.apache.calcite.sql.SqlFunctionCategory;
import com.qihoo.qsql.org.apache.calcite.sql.SqlKind;
import com.qihoo.qsql.org.apache.calcite.sql.type.OperandTypes;
import com.qihoo.qsql.org.apache.calcite.sql.type.ReturnTypes;
import com.qihoo.qsql.org.apache.calcite.util.Optionality;

import com.google.common.base.Preconditions;

/**
 * Definition of the <code>ANY_VALUE</code> aggregate functions,
 * returning any one of the values which go into it.
 */
public class SqlAnyValueAggFunction extends SqlAggFunction {

  //~ Instance fields --------------------------------------------------------

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlAnyValueAggFunction. */
  public SqlAnyValueAggFunction(SqlKind kind) {
    super(kind.name(),
        null,
        kind,
        ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM,
        false,
        false,
        Optionality.FORBIDDEN);
    Preconditions.checkArgument(kind == SqlKind.ANY_VALUE);
  }

  //~ Methods ----------------------------------------------------------------
}

// End SqlAnyValueAggFunction.java
