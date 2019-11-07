package com.qihoo.qsql.metadata.extern;

import com.qihoo.qsql.metadata.utils.MetadataUtil;
import com.qihoo.qsql.utils.PropertiesReader;
import java.util.Properties;
import org.junit.Test;

public class MetadataMainTest {

    @Test
    public void testMetadataMain() {
        Properties properties = PropertiesReader.readProperties("metadata.properties", MetadataUtil.class);
        if (! MetadataUtil.isEmbeddedDatabase(properties)) {
            String[] args = {"--dbType=mysql", "--action=delete"};
            MetadataMain.main(args);
        }

    }

}
