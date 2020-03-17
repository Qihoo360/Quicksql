package com.qihoo.qsql.plan;

import com.qihoo.qsql.exception.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link TableNameCollector}.
 */
public class TableNameCollectorTest {

    private static List<String> parseTableName(String sql) {
        TableNameCollector collector = new TableNameCollector();
        try {
            List<String> list = new ArrayList<>();
            list.addAll(new LinkedHashSet<String>(new ArrayList<>(collector
                .parseTableName(sql)
                .tableNames)));
            return list;
        } catch (SqlParseException ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    @Test
    public void testParseNoTableName() {
        String sql = "SELECT 1";
        Assert.assertEquals(new ArrayList<>(), parseTableName(sql));
    }

    @Test
    public void testParseNoTableNameWithColumnAlias() {
        String sql = "SELECT 1 a";
        Assert.assertEquals(new ArrayList<>(), parseTableName(sql));
    }

    @Test
    public void testParseSingleTableName() {
        String sql = "SELECT a1 FROM A";
        Assert.assertEquals(Collections.singletonList("A"), parseTableName(sql));
    }

    @Test
    public void testParseSingleTableNameWithAlias() {
        String sql = "SELECT a1 FROM A a";
        Assert.assertEquals(Collections.singletonList("A"), parseTableName(sql));
    }

    @Test
    public void testParseSingleTableNameWithAsAlias() {
        String sql = "SELECT a1 FROM A as a";
        Assert.assertEquals(Collections.singletonList("A"), parseTableName(sql));
    }

    @Test
    public void testParseSingleTableNameWithFilter() {
        String sql = "SELECT a1 FROM A as a WHERE id = 1";
        Assert.assertEquals(Collections.singletonList("A"), parseTableName(sql));
    }

    @Test
    public void testParseSingleTableNameWithAggregate() {
        String sql = "SELECT COUNT(a1) FROM A as a GROUP BY id";
        Assert.assertEquals(Collections.singletonList("A"), parseTableName(sql));
    }

    @Test
    public void testParseSingleTableNameWithSort() {
        String sql = "SELECT a1 FROM A as a ORDER BY id DESC";
        Assert.assertEquals(Collections.singletonList("A"), parseTableName(sql));
    }

    @Test
    public void testParseSingleTableNameWithLimit() {
        String sql = "SELECT a1 FROM A as a LIMIT 10";
        Assert.assertEquals(Collections.singletonList("A"), parseTableName(sql));
    }

    @Test
    public void testParseMixTableName() {
        String sql = "SELECT A.a1, B.b1 FROM A,B";
        Assert.assertEquals(Arrays.asList("A", "B"), parseTableName(sql));
    }

    @Test
    public void testParseMixTableNameWithSubSqlInSelect() {
        String sql = "SELECT A.a1, ( SELECT count(B.b1) FROM B ) FROM A";
        Assert.assertEquals(Arrays.asList("B", "A"), parseTableName(sql));
    }

    @Test
    public void testParseMixTableNameWithSubSqlInSelectPlusAlias() {
        String sql = "SELECT A.a1, ( SELECT count(B.b1) FROM B ) as BB FROM A";
        Assert.assertEquals(Arrays.asList("B", "A"), parseTableName(sql));
    }

    @Test
    public void testParseMixTableNameWithSubSqlInFrom() {
        String sql = "SELECT A.a1 FROM ( SELECT count(B.b1) as a1 FROM B ) as A";
        Assert.assertEquals(Collections.singletonList("B"), parseTableName(sql));
    }

    @Test
    public void testParseMixTableNameWithSubSqlInWhere() {
        String sql = "SELECT A.a1 FROM A WHERE A.id in (SELECT B.b1 as a1 FROM B)";
        Assert.assertEquals(Arrays.asList("A", "B"), parseTableName(sql));
    }

    @Test
    public void testParseMixTableNameWithJoin() {
        String sql = "SELECT AA.a1, BB.b1"
            + " FROM"
            + " ( SELECT a1 FROM A ) as AA"
            + " JOIN"
            + " ( SELECT b1 FROM B ) as BB"
            + " ON(AA.a1 = BB.b1)";
        Assert.assertEquals(Arrays.asList("A", "B"), parseTableName(sql));
    }

    @Test
    public void testParseMixTableNameWithUnion() {
        String sql = " ( SELECT a1 FROM A ) "
            + " UNION ( SELECT b1 FROM B )";
        Assert.assertEquals(Arrays.asList("A", "B"), parseTableName(sql));
    }

    //Since TableCollection will not validate sql when parse tableName, the sql below will success
    @Test
    public void testParseMixTableNameWithoutValidate() {
        String sql = "SELECT C.a1 FROM A, B";
        Assert.assertEquals(Arrays.asList("A", "B"), parseTableName(sql));
    }

    //Since TableCollection will parse sql first, the sql below will fail
    @Test
    public void testParseMixTableNameWithWrongSql() {
        String sql = "SELECT C.a1 LIMIT 10 FROM A, B";
        try {
            parseTableName(sql);
        } catch (RuntimeException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testParseInsertInto() {
        String sql = "INSERT INTO `hello` IN HDFS SELECT 1";
        TableNameCollector collector = new TableNameCollector();
        try {
            Assert.assertTrue(collector.parseTableName(sql).isDml());
        } catch (SqlParseException ex) {
            ex.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testParseSingleBdNameAndTableName() {
        String sql = "SELECT a1 FROM A.A";
        Assert.assertEquals(Collections.singletonList("A.A"), parseTableName(sql));
    }

    @Test
    public void testParseSingleBdNameAndTableNameWithError() {
        String sql = "SELECT a1 FROM A.A.A";
        try {
            parseTableName(sql);
        } catch (ParseException ex) {
            Assert.assertTrue(true);
        } catch (Exception ey) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testParseSingleBdNameAndTableNameWithError2() {
        String sql = "SELECT A.a1, ( SELECT count(B.b1) FROM B.B.B as B ) FROM A.A.A as A";
        try {
            parseTableName(sql);
        } catch (ParseException ex) {
            Assert.assertTrue(true);
        } catch (Exception ey) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testIncludeWithWordTableName() {
        String sql = "with A as (select * from B) select * from A";
        Assert.assertEquals(Collections.singletonList("B"), parseTableName(sql));
    }

    @Test
    public void testManyWithWordTableName() {
        String sql = "with A as (select * from B),C as (select * from B) select A.*,C.* from A,C";
        Assert.assertEquals(Collections.singletonList("B"), parseTableName(sql));
    }

    @Test
    public void testCaseWhenSubQueryTableName() {
        String sql = "select case when (select count(*) from store_sales where ss_quantity between 1 and 20) > 25437 "
            + "then (select avg(ss_ext_discount_amt) "
            + "from store_sales where ss_quantity between 1 and 20) else (select avg(ss_net_profit) "
            + "from test.store_sales where ss_quantity between 1 and 20) end bucket1  from reason where r_reason_sk ="
            + " 1";
        Assert.assertEquals(Arrays.asList("store_sales","reason"), parseTableName(sql));
    }

}
