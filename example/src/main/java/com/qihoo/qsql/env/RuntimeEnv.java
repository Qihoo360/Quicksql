package com.qihoo.qsql.env;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import com.qihoo.qsql.CsvJoinWithEsExample;
import com.qihoo.qsql.utils.PropertiesReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RuntimeEnv {
    static {
        PropertiesReader.configLogger();
    }
    private static final String TEST_DATA_URL = PropertiesReader.getTestDataFilePath();

    private static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

    public static void init() throws IOException {
        System.out.println("Elasticsearch Embedded Server is starting up, waiting....");
        final Map<String, String> mapping = ImmutableMap.of("stu_id", "keyword", "type", "keyword",
            "city", "keyword", "digest", "long", "province", "keyword");
        NODE.createIndex("student", mapping);

        // load records from file
        final List<ObjectNode> bulk = new ArrayList<>();
        Resources.readLines(CsvJoinWithEsExample.class.getResource("/student.json"),
            StandardCharsets.UTF_8, new LineProcessor<Void>() {
                @Override public boolean processLine(String line) throws IOException {
                    line = line.replaceAll("_id", "id");
                    bulk.add((ObjectNode) NODE.mapper().readTree(line));
                    return true;
                }

                @Override public Void getResult() {
                    return null;
                }
            });

        if (bulk.isEmpty()) {
            throw new IllegalStateException("No records to index. Empty file ?");
        }

        NODE.insertBulk("student", bulk);
        System.out.println("Elasticsearch Embedded Server has started!! Your query is running...");

    }

    public static final String metadata = "inline:\n"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"QSql\",\n"
        + "  \"schemas\": [{\n"
        + "      \"type\": \"custom\",\n"
        + "      \"name\": \"custom_name\",\n"
        + "      \"factory\": \"com.qihoo.qsql.org.apache.calcite.adapter.csv.CsvSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"\"\n"
        + "      },\n"
        + "      \"tables\": [{\n"
        + "        \"name\": \"depts\",\n"
        + "        \"type\": \"custom\",\n"
        + "        \"factory\": \"com.qihoo.qsql.org.apache.calcite.adapter.csv.CsvTableFactory\",\n"
        + "        \"operand\": {\n"
        + "          \"file\": \"" + TEST_DATA_URL + "\",\n"
        + "          \"flavor\": \"scannable\"\n"
        + "        },\n"
        + "        \"columns\": [{\n"
        + "            \"name\": \"deptno:int\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"name:string\"\n"
        + "          }\n"
        + "        ]\n"
        + "      }]\n"
        + "    },\n"
        + "    {\n"
        + "      \"type\": \"custom\",\n"
        + "      \"name\": \"student_profile\",\n"
        + "      \"factory\": \"com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch.ElasticsearchCustomSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"coordinates\": \"{'localhost': 9025}\",\n"
        + "        \"userConfig\": \"{'bulk.flush.max.actions': 10, 'bulk.flush.max.size.mb': 1,"
        + "'esUser':'username','esPass':'password'}\",\n"
        + "        \"index\": \"student\"\n"
        + "      },\n"
        + "      \"tables\": [{\n"
        + "        \"name\": \"student\",\n"
        + "        \"factory\": \"com.qihoo.qsql.org.apache.calcite.adapter.elasticsearch.ElasticsearchTableFactory\",\n"
        + "        \"operand\": {\n"
        + "          \"dbName\": \"student_profile\",\n"
        + "          \"tableName\": \"student\",\n"
        + "          \"esNodes\": \"localhost\",\n"
        + "          \"esPort\": \"9025\",\n"
        + "          \"esUser\": \"username\",\n"
        + "          \"esPass\": \"password\",\n"
        + "          \"esScrollNum\": \"246\",\n"
        + "          \"esIndex\": \"student\"\n"
        + "        },\n"
        + "        \"columns\": [{\n"
        + "            \"name\": \"city:string\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"province:string\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"digest:int\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"type:string\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"stuid:string\"\n"
        + "          }\n"
        + "        ]\n"
        + "      }]\n"
        + "    }\n"
        + "  ]\n"
        + "}";



    public static final String metaDataSQLIET = "inline:\n"
        + "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"QSql\",\n"
        + "  \"schemas\": [{\n"
        + "      \"type\": \"custom\",\n"
        + "      \"name\": \"main\",\n"
        + "      \"factory\": \"com.qihoo.qsql.org.apache.calcite.adapter.custom.JdbcSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "       \"dbName\": \"testdata\",\n"
        +"         \"tableName\": \"t_test_data\",\n"
        +"         \"dbType\": \"mysql\",\n"
        +"         \"jdbcDriver\": \"org.sqlite.JDBC\",\n"
        +"         \"jdbcUrl\": \"jdbc:sqlite:E://////test.db\",\n"
        +"         \"jdbcUser\": \"xx\",\n"
        +"         \"jdbcPassword\": \"xx\"\n"
        + "      },\n"
        + "      \"tables\": [{\n"
        + "        \"name\": \"t_test_data\",\n"
        + "        \"factory\": \"com.qihoo.qsql.org.apache.calcite.adapter.custom.JdbcTableFactory\",\n"
        + "        \"operand\": {\n"
        + "       \"dbName\": \"testdata\",\n"
        +"         \"tableName\": \"t_test_data\",\n"
        +"         \"dbType\": \"mysql\",\n"
        +"         \"jdbcDriver\": \"org.sqlite.JDBC\",\n"
        +"         \"jdbcUrl\": \"jdbc:sqlite:E://////test.db\",\n"
        +"         \"jdbcUser\": \"xx\",\n"
        +"         \"jdbcPassword\": \"xx\"\n"
        + "        },\n"
        + "        \"columns\": [{\n"
        + "            \"name\": \"id:int\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"dataid:int\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"data:string\"\n"
        + "          }\n"
        + "        ]\n"
        + "      }]\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    public static void close() {
        NODE.close();
    }

}
