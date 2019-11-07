package com.qihoo.qsql.metadata;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * MetadataPostmanTest.
 */
public class MetadataPostmanTest {

    //FIXME query without sensing case
    @Test
    public void testParseSimpleMetadata() {
        String meta = MetadataPostman.getCalciteModelSchema(
            Collections.singletonList("student")
        );
        validateSimpleName(meta, "student_profile", "student");
    }

    @Test
    public void testParseMultipleMetadata() {
        String meta = MetadataPostman.getCalciteModelSchema(
            Arrays.asList("homework_content", "department")
        );
        validateSimpleName(meta, "action_required", "homework_content");
        validateSimpleName(meta, "edu_manage", "department");
    }

    @Test
    public void testParseCompleteTableNameForMetadata() {
        String meta = MetadataPostman.getCalciteModelSchema(
            Collections.singletonList("action_required.homework_content")
        );
        validateSimpleName(meta, "action_required", "homework_content");
    }

    private void validateSimpleName(String meta, String dbName, String tableName) {
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(meta);
        JsonElement schemas = element.getAsJsonObject().get("schemas");
        Assert.assertTrue(schemas != null && schemas.getAsJsonArray().size() >= 1);
        JsonArray schemaArr = schemas.getAsJsonArray();
        List<JsonElement> elements = new ArrayList<>();
        schemaArr.forEach(elements::add);
        Assert.assertTrue(elements.stream()
            .anyMatch(el -> el.getAsJsonObject()
                .get("name").getAsString().equals(dbName)));
        Assert.assertTrue(elements.stream()
            .anyMatch(el -> {
                JsonArray arr = el.getAsJsonObject().get("tables").getAsJsonArray();
                List<JsonElement> tables = new ArrayList<>();
                arr.forEach(tables::add);
                return tables.stream()
                    .anyMatch(table -> table.getAsJsonObject().get("name")
                        .getAsString().equals(tableName));
            }));
    }
}
