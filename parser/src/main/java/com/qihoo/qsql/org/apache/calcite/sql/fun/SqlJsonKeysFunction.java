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
import com.qihoo.qsql.org.apache.calcite.sql.type.OperandTypes;
import com.qihoo.qsql.org.apache.calcite.sql.type.ReturnTypes;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlTypeFamily;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlTypeTransforms;

/**
 * The <code>JSON_KEYS</code> function.
 */
public class SqlJsonKeysFunction extends SqlFunction {
  public SqlJsonKeysFunction() {
    super("JSON_KEYS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.cascade(ReturnTypes.VARCHAR_2000, SqlTypeTransforms.FORCE_NULLABLE),
          null,
          OperandTypes.or(OperandTypes.ANY,
              OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER)),
          SqlFunctionCategory.SYSTEM);
  }
}

// End SqlJsonKeysFunction.java
