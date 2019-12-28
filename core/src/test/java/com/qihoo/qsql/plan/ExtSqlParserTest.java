package com.qihoo.qsql.plan;

import com.qihoo.qsql.utils.SqlUtil;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import com.qihoo.qsql.org.apache.calcite.sql.SqlNode;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParseException;
import com.qihoo.qsql.org.apache.calcite.sql.parser.SqlParser;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlConformance;
import com.qihoo.qsql.org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ExtSqlParserTest}.
 */
public class ExtSqlParserTest {

    private SqlConformance conformance = SqlConformanceEnum.MYSQL_5;
    private Quoting quoting = Quoting.BACK_TICK;

    private SqlParser.Config config = SqlParser
        .configBuilder()
        .setConformance(conformance)
        .setQuoting(quoting)
        .setQuotedCasing(Casing.UNCHANGED)
        .setUnquotedCasing(Casing.UNCHANGED)
        .build();

    @Test
    public void testBasicFunction() {
        check("INSERT INTO dbName.tableName(col) IN MySQL SELECT 1 as col");
    }

    @Test
    public void testWriteToHdfs() {
        check("INSERT INTO `hdfs://cluster:9090/hello/world` IN HDFS SELECT 1 as col LIMIT 10", true);
    }

    @Test
    public void testWriteToEs() {
        check("INSERT INTO index.type(col1, col2, col3) IN Elasticsearch SELECT 'one', 'two', 'three'");
    }

    @Test
    public void testCompoundQuery() {
        check("INSERT INTO `/hello/world` IN HDFS SELECT * FROM tab WHERE tab.col LIKE '%all%'");
    }

    @Test
    public void testDmlJudgement() {
        check("INSERT INTO `hdfs://cluster:9090/hello/world` IN HDFS SELECT 1 as col LIMIT 10", true);
        check("SELECT 1 as col LIMIT 10", false);
    }

    private void check(String sql) {
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode root = null;
        try {
            root = parser.parseQuery();
        } catch (SqlParseException ex) {
            ex.printStackTrace();
            Assert.assertTrue(false);
        }
        Assert.assertEquals(root.getClass().getSimpleName(), "SqlInsertOutput");
    }

    private void check(String sql, boolean isDml) {
        QueryTables tables = SqlUtil.parseTableName(sql);
        Assert.assertEquals(tables.isDml(), isDml);
    }
}
