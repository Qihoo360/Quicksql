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
/*
 * Modifications copyright (C) 2018 QSQL
 */
package com.qihoo.qsql.org.apache.calcite.schema.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qihoo.qsql.org.apache.calcite.config.CalciteConnectionConfig;
import com.qihoo.qsql.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import com.qihoo.qsql.org.apache.calcite.model.*;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataType;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelDataTypeFactory;
import com.qihoo.qsql.org.apache.calcite.schema.Schema;
import com.qihoo.qsql.org.apache.calcite.schema.Statistic;
import com.qihoo.qsql.org.apache.calcite.schema.Statistics;
import com.qihoo.qsql.org.apache.calcite.schema.Table;
import com.qihoo.qsql.org.apache.calcite.schema.Wrapper;
import com.qihoo.qsql.org.apache.calcite.sql.SqlCall;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.util.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for implementing {@link Table}.
 *
 * <p>Sub-classes should override {@link #isRolledUp} and
 * {@link Table#rolledUpColumnValidInsideAgg(String, SqlCall, SqlNode, CalciteConnectionConfig)} if their table can
 * potentially contain rolled up values. This information is used by the validator to check for illegal uses of these
 * columns.
 */
public abstract class AbstractTable implements Table, Wrapper {

    protected AbstractTable() {
    }

    // Default implementation. Override if you have statistics.
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        return null;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column,
        SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        return true;
    }

    //Modified by QSQL.
    protected RelDataType getRowType(String modelUri, String dbName,
        String tableName, RelDataTypeFactory relDataTypeFactory) {
        final ObjectMapper mapper = new ObjectMapper();
        JsonRoot root;
        try {
            if (modelUri.startsWith("inline:")) {
                root = mapper.readValue(modelUri.substring("inline:".length()), JsonRoot.class);
            } else {
                root = mapper.readValue(new File(modelUri), JsonRoot.class);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<JsonColumn> columns = new ArrayList<>();
        for (JsonSchema schema : root.schemas) {
            if (dbName.toLowerCase().equals(schema.name.toLowerCase())) {
                JsonCustomSchema jsonCustomSchema = (JsonCustomSchema) schema;
                for (JsonTable table : jsonCustomSchema.tables) {
                    if (tableName.toLowerCase().equals(table.name.toLowerCase())) {
                        columns = table.columns;
                    }
                }
            }
        }

        //Support column type: int, varchar, tinyint, float, double, long, boolean, array, map. not sensitive case
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();
        JavaTypeFactoryImpl javaTypeFactory = new JavaTypeFactoryImpl();
        for (JsonColumn column : columns) {
            String[] array = column.name.split(":", -1);
            names.add(array[0]);
            types.add(javaTypeFactory.getDataType(relDataTypeFactory, array[1]));
        }
        return relDataTypeFactory.createStructType(Pair.zip(names, types));
    }
}