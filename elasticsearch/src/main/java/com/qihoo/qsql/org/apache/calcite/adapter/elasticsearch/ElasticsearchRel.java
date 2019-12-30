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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import com.qihoo.qsql.org.apache.calcite.plan.Convention;
import com.qihoo.qsql.org.apache.calcite.plan.RelOptTable;
import com.qihoo.qsql.org.apache.calcite.rel.RelFieldCollation;
import com.qihoo.qsql.org.apache.calcite.rel.RelNode;
import com.qihoo.qsql.org.apache.calcite.rel.type.RelRecordType;
import com.qihoo.qsql.org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Relational expression that uses Elasticsearch calling convention.
 */
public interface ElasticsearchRel extends RelNode {

    void implement(Implementor implementor);

    /**
     * Calling convention for relational operations that occur in Elasticsearch.
     */
    Convention CONVENTION = new Convention.Impl("ELASTICSEARCH", ElasticsearchRel.class);

    /**
     * Callback for the implementation process that converts a tree of {@link ElasticsearchRel} nodes into an
     * Elasticsearch query.
     */
    class Implementor {

        final List<String> list = new ArrayList<>();

        /**
         * Sorting clauses.
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html">Sort</a>
         */
        final List<Map.Entry<String, RelFieldCollation.Direction>> sort = new ArrayList<>();

        /**
         * Elastic aggregation ({@code MIN / MAX / COUNT} etc.) statements (functions).
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html">aggregations</a>
         */
        final List<Map.Entry<String, String>> aggregations = new ArrayList<>();

        /**
         * Allows bucketing documents together. Similar to {@code select ... from table group by field1}
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/6.3/search-aggregations-bucket.html">Bucket
         * Aggregrations</a>
         */
        final List<String> groupBy = new ArrayList<>();

        /**
         * Starting index (default {@code 0}). Equivalent to {@code start} in ES query.
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html">From/Size</a>
         */
        Long offset;

        /**
         * Number of records to return. Equivalent to {@code size} in ES query.
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html">From/Size</a>
         */
        Long fetch;

        RelOptTable table;
        ElasticsearchTable elasticsearchTable;

        void add(String findOp) {
            list.add(findOp);
        }

        void addGroupBy(String field) {
            Objects.requireNonNull(field, "field");
            groupBy.add(field);
        }

        void addSort(String field, RelFieldCollation.Direction direction) {
            Objects.requireNonNull(field, "field");
            sort.add(new Pair<>(field, direction));
        }

        void addAggregation(String field, String expression) {
            Objects.requireNonNull(field, "field");
            Objects.requireNonNull(expression, "expression");
            aggregations.add(new Pair<>(field, expression));
        }

        void offset(long offset) {
            this.offset = offset;
        }

        void fetch(long fetch) {
            this.fetch = fetch;
        }

        void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((ElasticsearchRel) input).implement(this);
        }

        //Updated by qsql-team
        public String convert(RelNode input, List<Pair<String, Class>> fields) throws IOException {
            ((ElasticsearchRel) input).implement(this);

            ObjectMapper mapper = new ObjectMapper();
            if (! aggregations.isEmpty()) {
                return aggregate(fields, mapper);
            }

            final ObjectNode query = mapper.createObjectNode();
            // manually parse from previously concatenated string
            for (String op : list) {
                query.setAll((ObjectNode) mapper.readTree(op));
            }

            if (! sort.isEmpty()) {
                ArrayNode sortNode = query.withArray("sort");
                sort.forEach(e ->
                    sortNode.add(
                        mapper.createObjectNode().put(e.getKey(), e.getValue().isDescending() ? "desc" : "asc"))
                );
            }

            if (offset != null) {
                query.put("from", offset);
            }

            if (fetch != null) {
                query.put("size", fetch);
            }

            return query.toString();
        }

        //Updated by qsql-team, copy from ElaticsearchTable, extract common code after all
        private String aggregate(List<Pair<String, Class>> fields, ObjectMapper mapper) throws IOException {
            if (aggregations.isEmpty()) {
                throw new IllegalArgumentException("Missing Aggregations");
            }

            if (! groupBy.isEmpty() && offset != null) {
                String message = "Currently ES doesn't support generic pagination "
                    + "with aggregations. You can still use LIMIT keyword (without OFFSET). "
                    + "For more details see https://github.com/elastic/elasticsearch/issues/4915";
                throw new IllegalStateException(message);
            }

            final ObjectNode query = mapper.createObjectNode();
            // manually parse into JSON from previously concatenated strings
            for (String op : list) {
                query.setAll((ObjectNode) mapper.readTree(op));
            }

            // remove / override attributes which are not applicable to aggregations
            query.put("_source", false);
            query.put("size", 0);
            query.remove("script_fields");

            // allows to detect aggregation for count(*)
            final Predicate<Entry<String, String>> isCountStar = e -> e.getValue()
                .contains("\"" + ElasticsearchConstants.ID + "\"");

            // list of expressions which are count(*)
            final Set<String> countAll = aggregations.stream()
                .filter(isCountStar)
                .map(Map.Entry::getKey).collect(Collectors.toSet());

            // due to ES aggregation format. fields in "order by" clause should go first
            // if "order by" is missing. order in "group by" is un-important
            final Set<String> orderedGroupBy = new LinkedHashSet<>();
            orderedGroupBy.addAll(sort.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
            orderedGroupBy.addAll(groupBy);

            // construct nested aggregations node(s)
            ObjectNode parent = query.with("aggregations");
            for (String name : orderedGroupBy) {
                final String aggName = "g_" + name;

                final ObjectNode section = parent.with(aggName);
                final ObjectNode terms = section.with("terms");
                terms.put("field", name);
                terms.set("missing", ElasticsearchJson.MISSING_VALUE); // expose missing terms

                if (fetch != null) {
                    terms.put("size", fetch);
                }

                sort.stream().filter(e -> e.getKey().equals(name)).findAny().ifPresent(s -> {
                    terms.with("order").put("_key", s.getValue().isDescending() ? "desc" : "asc");
                });

                parent = section.with("aggregations");
            }

            // simple version for queries like "select count(*), max(col1) from table" (no GROUP BY cols)
            if (! groupBy.isEmpty() || ! aggregations.stream().allMatch(isCountStar)) {
                for (Map.Entry<String, String> aggregation : aggregations) {
                    JsonNode value = mapper.readTree(aggregation.getValue());
                    parent.set(aggregation.getKey(), value);
                }
            }

            // cleanup query. remove empty AGGREGATIONS element (if empty)
            JsonNode agg = query;
            while (agg.has("aggregations") && agg.get("aggregations").elements().hasNext()) {
                agg = agg.get("aggregations");
            }
            ((ObjectNode) agg).remove("aggregations");

            return query.toString();
        }
    }
}

// End ElasticsearchRel.java
