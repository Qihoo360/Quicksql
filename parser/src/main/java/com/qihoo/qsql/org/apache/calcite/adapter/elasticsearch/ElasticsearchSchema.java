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
package com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch;

import com.qihoo.qsql.org.apache.calcite.schema.Table;
import com.qihoo.qsql.org.apache.calcite.schema.impl.AbstractSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * Schema mapped onto an index of ELASTICSEARCH.
 *
 * <p>Each table in the schema is an ELASTICSEARCH index.
 */
public class ElasticsearchSchema extends AbstractSchema {

  private final String index;

  private final RestClient client;

  private final ObjectMapper mapper;

  private final Map<String, Table> tableMap;



  //Updated by qsql-team
  public ElasticsearchSchema(RestClient client, String index) {
    this.client = Objects.requireNonNull(client, "client");
    this.mapper = new ObjectMapper();
    this.index = Objects.requireNonNull(index, "index");
    this.tableMap = new HashMap<>();
  }
  //Updated by qsql-team
  public RestClient getClient() {
    return client;
  }

  public void addTable(String index, Table table) {
    if(tableMap instanceof ImmutableMap)
      throw new RuntimeException("error metadata class");
    tableMap.put(index, table);
  }

  /**
   * Allows schema to be instantiated from existing elastic search client.
   * This constructor is used in tests.
   * @param client existing client instance
   * @param mapper mapper for JSON (de)serialization
   * @param index name of ES index
   */

  public ElasticsearchSchema(RestClient client, ObjectMapper mapper, String index) {
    super();
    this.client = Objects.requireNonNull(client, "client");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.index = Objects.requireNonNull(index, "index");
    try {
      this.tableMap = createTables(indicesFromElastic());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  private Map<String, Table> createTables(Iterable<String> indices) {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (String index : indices) {
      builder.put(index, new ElasticsearchTable(client, mapper, index));
    }
    return builder.build();
  }

  /**
   * Queries {@code _alias} definition to automatically detect all indices
   *
   * @return list of indices
   * @throws IOException for any IO related issues
   * @throws IllegalStateException if reply is not understood
   */
  private Set<String> indicesFromElastic() throws IOException {
    final String endpoint = "/_alias";
    final Response response = client.performRequest(new Request("GET", endpoint));
    try (InputStream is = response.getEntity().getContent()) {
      final JsonNode root = mapper.readTree(is);
      if (!(root.isObject() && root.size() > 0)) {
        final String message = String.format(Locale.ROOT, "Invalid response for %s/%s "
                + "Expected object of at least size 1 got %s (of size %d)", response.getHost(),
            response.getRequestLine(), root.getNodeType(), root.size());
        throw new IllegalStateException(message);
      }

      return Sets.newHashSet(root.fieldNames());
    }
  }

  public String getIndex() {
    return index;
  }
}

// End ElasticsearchSchema.java
