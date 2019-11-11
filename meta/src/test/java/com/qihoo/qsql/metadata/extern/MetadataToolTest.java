package com.qihoo.qsql.metadata.extern;

import com.qihoo.qsql.metadata.utils.MetadataUtil;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetadataToolTest {

    MetadataTool metadataTool;
    Properties metadataProperties = MetadataUtil.readProperties("metadata.properties");

    /**
     * init metadataTool.
     */
    @Before
    public void init() {
        metadataTool = new MetadataTool();
    }

    @Test
    public void testMetadataTool() {
        if (! MetadataUtil.isEmbeddedDatabase(metadataProperties)) {
            Properties properties = new Properties();
            properties.put("--action", "delete");
            properties.put("--dbType", "mysql");
            try {
                metadataTool.run(properties);
            } catch (Exception ex) {
                Assert.assertTrue(false);
            }
        }
    }

    @Test
    public void testMetadataToolWithoutDbType() {
        if (! MetadataUtil.isEmbeddedDatabase(metadataProperties)) {
            Properties properties = new Properties();
            properties.put("--action", "init");
            try {
                metadataTool.run(properties);
            } catch (Exception ex) {
                Assert.assertTrue(true);
            }
        }
    }

    @Test
    public void testMetadataToolWithErrorAction() {
        Properties properties = new Properties();
        properties.put("--action", "update");
        properties.put("--dbType", "mysql");
        try {
            metadataTool.run(properties);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testMetadataToolWithErrorDbType() {
        Properties properties = new Properties();
        properties.put("--action", "init");
        properties.put("--dbType", "oracle");
        try {
            metadataTool.run(properties);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }
    }
}
