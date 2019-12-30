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
package com.qihoo.qsql.org.apache.calcite.prepare;

import com.qihoo.qsql.org.apache.calcite.config.CalciteConnectionConfig;
import com.qihoo.qsql.org.apache.calcite.jdbc.CalciteSchema;
import com.qihoo.qsql.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import com.qihoo.qsql.org.apache.calcite.model.ModelHandler;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptPlanner;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeFactory;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeField;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeSystem;
import com.qihoo.qsql.org.apache.calcite.schema.AggregateFunction;
import com.qihoo.qsql.org.apache.calcite.schema.Function;
import com.qihoo.qsql.org.apache.calcite.schema.FunctionParameter;
import com.qihoo.qsql.org.apache.calcite.schema.ScalarFunction;
import com.qihoo.qsql.org.apache.calcite.schema.Table;
import com.qihoo.qsql.org.apache.calcite.schema.TableFunction;
import com.qihoo.qsql.org.apache.calcite.schema.TableMacro;
import com.qihoo.qsql.org.apache.calcite.schema.Wrapper;
import com.qihoo.qsql.org.apache.calcite.schema.impl.ScalarFunctionImpl;
import com.qihoo.qsql.org.apache.calcite.sql.SqlFunctionCategory;
import com.qihoo.qsql.org.apache.calcite.sql.SqlIdentifier;
import com.qihoo.qsql.org.apache.calcite.sql.SqlOperator;
import com.qihoo.qsql.org.apache.calcite.sql.SqlOperatorTable;
import com.qihoo.qsql.org.apache.calcite.sql.SqlSyntax;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParserPos;
import com.qihoo.qsql.org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import com.qihoo.qsql.org.apache.calcite.sql.type.InferTypes;
import com.qihoo.qsql.org.apache.calcite.sql.type.OperandTypes;
import com.qihoo.qsql.org.apache.calcite.sql.type.ReturnTypes;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlReturnTypeInference;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlTypeFamily;
import com.qihoo.qsql.org.apache.calcite.sql.type.SqlTypeName;
import com.qihoo.qsql.org.apache.calcite.sql.util.ListSqlOperatorTable;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlMoniker;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlMonikerImpl;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlMonikerType;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlNameMatcher;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlNameMatchers;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlValidatorUtil;
import com.qihoo.qsql.org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Implementation of {@link com.qihoo.qsql.org.apache.calcite.prepare.Prepare.CatalogReader}
 * and also {@link com.qihoo.qsql.org.apache.calcite.sql.SqlOperatorTable} based on tables and
 * functions defined schemas.
 */
public class CalciteCatalogReader implements Prepare.CatalogReader {
  protected final CalciteSchema rootSchema;
  protected final RelDataTypeFactory typeFactory;
  private final List<List<String>> schemaPaths;
  protected final SqlNameMatcher nameMatcher;
  protected final CalciteConnectionConfig config;

  public CalciteCatalogReader(CalciteSchema rootSchema,
      List<String> defaultSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
    this(rootSchema, SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
        ImmutableList.of(Objects.requireNonNull(defaultSchema),
            ImmutableList.of()),
        typeFactory, config);
  }

  protected CalciteCatalogReader(CalciteSchema rootSchema,
      SqlNameMatcher nameMatcher, List<List<String>> schemaPaths,
      RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
    this.rootSchema = Objects.requireNonNull(rootSchema);
    this.nameMatcher = nameMatcher;
    this.schemaPaths =
        Util.immutableCopy(Util.isDistinct(schemaPaths)
            ? schemaPaths
            : new LinkedHashSet<>(schemaPaths));
    this.typeFactory = typeFactory;
    this.config = config;
  }

  public CalciteCatalogReader withSchemaPath(List<String> schemaPath) {
    return new CalciteCatalogReader(rootSchema, nameMatcher,
        ImmutableList.of(schemaPath, ImmutableList.of()), typeFactory, config);
  }

  public Prepare.PreparingTable getTable(final List<String> names) {
    // First look in the default schema, if any.
    // If not found, look in the root schema.
    CalciteSchema.TableEntry entry = SqlValidatorUtil.getTableEntry(this, names);
    if (entry != null) {
      final Table table = entry.getTable();
      if (table instanceof Wrapper) {
        final Prepare.PreparingTable relOptTable =
            ((Wrapper) table).unwrap(Prepare.PreparingTable.class);
        if (relOptTable != null) {
          return relOptTable;
        }
      }
      return RelOptTableImpl.create(this,
          table.getRowType(typeFactory), entry, null);
    }
    return null;
  }

  @Override public CalciteConnectionConfig getConfig() {
    return config;
  }

  private Collection<Function> getFunctionsFrom(List<String> names) {
    final List<Function> functions2 = new ArrayList<>();
    final List<List<String>> schemaNameList = new ArrayList<>();
    if (names.size() > 1) {
      // Name qualified: ignore path. But we do look in "/catalog" and "/",
      // the last 2 items in the path.
      if (schemaPaths.size() > 1) {
        schemaNameList.addAll(Util.skip(schemaPaths));
      } else {
        schemaNameList.addAll(schemaPaths);
      }
    } else {
      for (List<String> schemaPath : schemaPaths) {
        CalciteSchema schema =
            SqlValidatorUtil.getSchema(rootSchema, schemaPath, nameMatcher);
        if (schema != null) {
          schemaNameList.addAll(schema.getPath());
        }
      }
    }
    for (List<String> schemaNames : schemaNameList) {
      CalciteSchema schema =
          SqlValidatorUtil.getSchema(rootSchema,
              Iterables.concat(schemaNames, Util.skipLast(names)), nameMatcher);
      if (schema != null) {
        final String name = Util.last(names);
        functions2.addAll(schema.getFunctions(name, true));
      }
    }
    return functions2;
  }

  public RelDataType getNamedType(SqlIdentifier typeName) {
    CalciteSchema.TypeEntry typeEntry = SqlValidatorUtil.getTypeEntry(getRootSchema(), typeName);
    if (typeEntry != null) {
      return typeEntry.getType().apply(typeFactory);
    } else {
      return null;
    }
  }

  public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
    final CalciteSchema schema =
        SqlValidatorUtil.getSchema(rootSchema, names, nameMatcher);
    if (schema == null) {
      return ImmutableList.of();
    }
    final List<SqlMoniker> result = new ArrayList<>();

    // Add root schema if not anonymous
    if (!schema.name.equals("")) {
      result.add(moniker(schema, null, SqlMonikerType.SCHEMA));
    }

    final Map<String, CalciteSchema> schemaMap = schema.getSubSchemaMap();

    for (String subSchema : schemaMap.keySet()) {
      result.add(moniker(schema, subSchema, SqlMonikerType.SCHEMA));
    }

    for (String table : schema.getTableNames()) {
      result.add(moniker(schema, table, SqlMonikerType.TABLE));
    }

    final NavigableSet<String> functions = schema.getFunctionNames();
    for (String function : functions) { // views are here as well
      result.add(moniker(schema, function, SqlMonikerType.FUNCTION));
    }
    return result;
  }

  private SqlMonikerImpl moniker(CalciteSchema schema, String name,
      SqlMonikerType type) {
    final List<String> path = schema.path(name);
    if (path.size() == 1
        && !schema.root().name.equals("")
        && type == SqlMonikerType.SCHEMA) {
      type = SqlMonikerType.CATALOG;
    }
    return new SqlMonikerImpl(path, type);
  }

  public List<List<String>> getSchemaPaths() {
    return schemaPaths;
  }

  public Prepare.PreparingTable getTableForMember(List<String> names) {
    return getTable(names);
  }

  @SuppressWarnings("deprecation")
  public RelDataTypeField field(RelDataType rowType, String alias) {
    return nameMatcher.field(rowType, alias);
  }

  @SuppressWarnings("deprecation")
  public boolean matches(String string, String name) {
    return nameMatcher.matches(string, name);
  }

  public RelDataType createTypeFromProjection(final RelDataType type,
      final List<String> columnNameList) {
    return SqlValidatorUtil.createTypeFromProjection(type, columnNameList,
        typeFactory, nameMatcher.isCaseSensitive());
  }

  public void lookupOperatorOverloads(final SqlIdentifier opName,
      SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList) {
    if (syntax != SqlSyntax.FUNCTION) {
      return;
    }

    final Predicate<Function> predicate;
    if (category == null) {
      predicate = function -> true;
    } else if (category.isTableFunction()) {
      predicate = function ->
          function instanceof TableMacro
              || function instanceof TableFunction;
    } else {
      predicate = function ->
          !(function instanceof TableMacro
              || function instanceof TableFunction);
    }
    getFunctionsFrom(opName.names)
        .stream()
        .filter(predicate)
        .map(function -> toOp(opName, function))
        .forEachOrdered(operatorList::add);
  }

  /** Creates an operator table that contains functions in the given class.
   *
   * @see ModelHandler#addFunctions */
  public static SqlOperatorTable operatorTable(String className) {
    // Dummy schema to collect the functions
    final CalciteSchema schema =
        CalciteSchema.createRootSchema(false, false);
    ModelHandler.addFunctions(schema.plus(), null, ImmutableList.of(),
        className, "*", true);

    // The following is technical debt; see [CALCITE-2082] Remove
    // RelDataTypeFactory argument from SqlUserDefinedAggFunction constructor
    final SqlTypeFactoryImpl typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    final ListSqlOperatorTable table = new ListSqlOperatorTable();
    for (String name : schema.getFunctionNames()) {
      for (Function function : schema.getFunctions(name, true)) {
        final SqlIdentifier id = new SqlIdentifier(name, SqlParserPos.ZERO);
        table.add(
            toOp(typeFactory, id, function));
      }
    }
    return table;
  }

  private SqlOperator toOp(SqlIdentifier name, final Function function) {
    return toOp(typeFactory, name, function);
  }

  /** Converts a function to a {@link com.qihoo.qsql.org.apache.calcite.sql.SqlOperator}.
   *
   * <p>The {@code typeFactory} argument is technical debt; see [CALCITE-2082]
   * Remove RelDataTypeFactory argument from SqlUserDefinedAggFunction
   * constructor. */
  private static SqlOperator toOp(RelDataTypeFactory typeFactory,
      SqlIdentifier name, final Function function) {
    List<RelDataType> argTypes = new ArrayList<>();
    List<SqlTypeFamily> typeFamilies = new ArrayList<>();
    for (FunctionParameter o : function.getParameters()) {
      final RelDataType type = o.getType(typeFactory);
      argTypes.add(type);
      typeFamilies.add(
          Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
    }
    final FamilyOperandTypeChecker typeChecker =
        OperandTypes.family(typeFamilies, i ->
            function.getParameters().get(i).isOptional());
    final List<RelDataType> paramTypes = toSql(typeFactory, argTypes);
    if (function instanceof ScalarFunction) {
      return new SqlUserDefinedFunction(name, infer((ScalarFunction) function),
          InferTypes.explicit(argTypes), typeChecker, paramTypes, function);
    } else if (function instanceof AggregateFunction) {
      return new SqlUserDefinedAggFunction(name,
          infer((AggregateFunction) function), InferTypes.explicit(argTypes),
          typeChecker, (AggregateFunction) function, false, false, typeFactory);
    } else if (function instanceof TableMacro) {
      return new SqlUserDefinedTableMacro(name, ReturnTypes.CURSOR,
          InferTypes.explicit(argTypes), typeChecker, paramTypes,
          (TableMacro) function);
    } else if (function instanceof TableFunction) {
      return new SqlUserDefinedTableFunction(name, ReturnTypes.CURSOR,
          InferTypes.explicit(argTypes), typeChecker, paramTypes,
          (TableFunction) function);
    } else {
      throw new AssertionError("unknown function type " + function);
    }
  }

  private static SqlReturnTypeInference infer(final ScalarFunction function) {
    return opBinding -> {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType type;
      if (function instanceof ScalarFunctionImpl) {
        type = ((ScalarFunctionImpl) function).getReturnType(typeFactory,
            opBinding);
      } else {
        type = function.getReturnType(typeFactory);
      }
      return toSql(typeFactory, type);
    };
  }

  private static SqlReturnTypeInference infer(
      final AggregateFunction function) {
    return opBinding -> {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType type = function.getReturnType(typeFactory);
      return toSql(typeFactory, type);
    };
  }

  private static List<RelDataType> toSql(
      final RelDataTypeFactory typeFactory, List<RelDataType> types) {
    return Lists.transform(types, type -> toSql(typeFactory, type));
  }

  private static RelDataType toSql(RelDataTypeFactory typeFactory,
      RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType
        && ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass()
        == Object.class) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.ANY), true);
    }
    return JavaTypeFactoryImpl.toSql(typeFactory, type);
  }

  public List<SqlOperator> getOperatorList() {
    return null;
  }

  public CalciteSchema getRootSchema() {
    return rootSchema;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public void registerRules(RelOptPlanner planner) throws Exception {
  }

  @SuppressWarnings("deprecation")
  @Override public boolean isCaseSensitive() {
    return nameMatcher.isCaseSensitive();
  }

  public SqlNameMatcher nameMatcher() {
    return nameMatcher;
  }

  @Override public <C> C unwrap(Class<C> aClass) {
    if (aClass.isInstance(this)) {
      return aClass.cast(this);
    }
    return null;
  }
}

// End CalciteCatalogReader.java
