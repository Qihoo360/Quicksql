package com.qihoo.qsql.metadata;


import com.qihoo.qsql.metadata.entity.DatabaseParamValue;
import com.qihoo.qsql.metadata.entity.DatabaseValue;
import com.qihoo.qsql.metadata.entity.TableValue;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MetadataClientTest {

    private static MetadataClient client = null;

    //FIXME query without sensing case
    @BeforeClass
    public static void open() throws SQLException {
        client = new MetadataClient();
    }

    @Test
    public void testQueryBasicDatabaseInfo() {
        DatabaseValue value = client.getBasicDatabaseInfo("student-profile");
        Assert.assertEquals("es", value.getDbType());
        Assert.assertEquals("student-profile", value.getName());
    }

    @Test
    public void testAccessEmbeddedDatabaseByMultipleClient() {
        try (MetadataClient otherClient = new MetadataClient()) {
            DatabaseValue first = otherClient.getBasicDatabaseInfo("student-profile");
            DatabaseValue second = client.getBasicDatabaseInfo("student-profile");
            Assert.assertNotNull(first);
            Assert.assertNotNull(second);
            Assert.assertEquals(first, second);
        } catch (SQLException ex) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testAccessEmbeddedDatabaseConcurrently() throws InterruptedException {
        BlockingQueue<DatabaseValue> queue = new ArrayBlockingQueue<>(10);
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                try (MetadataClient client = new MetadataClient()) {
                    DatabaseValue value = client.getBasicDatabaseInfo("student-profile");
                    queue.offer(value);
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            });
            thread.start();
            thread.join();
        }

        DatabaseValue value = client.getBasicDatabaseInfo("student-profile");
        DatabaseValue[] values = new DatabaseValue[10];
        Arrays.fill(values, value);
        Assert.assertArrayEquals(values, queue.toArray());
    }

    @Test
    public void testQueryDatabaseSchema() {
        DatabaseValue value = client.getBasicDatabaseInfo("student-profile");
        List<DatabaseParamValue> params = client.getDatabaseSchema(value.getDbId());
        params.forEach(param -> Assert.assertTrue(
            param.getParamKey().contains("es") && ! param.getParamValue().isEmpty()));
    }

    @Test
    public void testQueryTableSchema() {
        DatabaseValue value = client.getBasicDatabaseInfo("student-profile");
        List<TableValue> tables = client.getTableSchema("STUDENT");
        tables.forEach(table -> System.out.println(table.getTblName()));
    }

    @Test
    public void testQueryFieldsWithOrder() {
        List<TableValue> tableValues = client.getTableSchema("student");
        List<ColumnValue> columns = client.getFieldsSchema(tableValues.get(0).getTblId());
        Assert.assertTrue(columns.size() == 5);
        Assert.assertTrue(columns.get(0).getColumnName().equals("city"));
        Assert.assertTrue(columns.get(1).getColumnName().equals("province"));
        Assert.assertTrue(columns.get(2).getColumnName().equals("digest"));
        Assert.assertTrue(columns.get(3).getColumnName().equals("type"));
        Assert.assertTrue(columns.get(4).getColumnName().equals("stu_id"));
    }

    @Test
    public void testQueryNonExistedTableSchemaByTableName() {
        List<TableValue> tables = client.getTableSchema("NOT_EXISTED_TABLE");
        Assert.assertTrue(tables.isEmpty());
    }

    @Test
    public void testQueryCompleteSchema() {
        DatabaseValue databaseValue = client.getBasicDatabaseInfoById(1L);
        List<DatabaseParamValue> params = client.getDatabaseSchema(databaseValue.getDbId());
        Assert.assertTrue(! params.isEmpty());
        params.forEach(param -> Assert.assertTrue(param.getParamKey().contains("es")));
        List<TableValue> tableValues = client.getTableSchema("student");
        Assert.assertTrue(tableValues.size() == 1);
        List<ColumnValue> columns = client.getFieldsSchema(tableValues.get(0).getTblId());
        Assert.assertTrue(! columns.isEmpty());
        columns.forEach(
            column -> Assert.assertTrue(
                ! column.getColumnName().isEmpty() && ! column.getTypeName().isEmpty()));
    }

    /**
     * close resource.
     */
    @AfterClass
    public static void close() {
        if (client != null) {
            client.close();
        }
    }
}
