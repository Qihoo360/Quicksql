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

import com.qihoo.qsql.org.apache.calcite.sql.SqlFunction;
import com.qihoo.qsql.org.apache.calcite.sql.SqlFunctionCategory;
import com.qihoo.qsql.org.apache.calcite.sql.SqlKind;
import com.qihoo.qsql.org.apache.calcite.sql.SqlOperatorBinding;
import com.qihoo.qsql.org.apache.calcite.sql.SqlSyntax;
import com.qihoo.qsql.org.apache.calcite.sql.type.OperandTypes;
import com.qihoo.qsql.org.apache.calcite.sql.type.ReturnTypes;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * The <code>CURRENT_DATE</code> function.
 */
public class SqlCurrentDateFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlCurrentDateFunction() {
    super(
        "CURRENT_DATE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE,
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION_ID;
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return SqlMonotonicity.INCREASING;
  }

  // Plans referencing context variables should never be cached
  public boolean isDynamicFunction() {
    return true;
  }
}

// End SqlCurrentDateFunction.java
