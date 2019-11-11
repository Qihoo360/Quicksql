package com.qihoo.qsql.metadata.utils;

import com.qihoo.qsql.metadata.MetadataParams;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

/**
 * Utils for make connection with extern storage.
 */
public class MetadataUtil {

    /**
     * .
     */
    public static boolean isEmbeddedDatabase(Properties properties) {
        return properties.getProperty(MetadataParams.META_STORAGE_MODE, "other")
            .toLowerCase().equals("intern");
    }

    /**
     * Create JDBC connection based on properties.
     *
     * @param properties params for jdbc connection
     * @return JDBC Connection
     */
    public static Connection getExternalConnection(Properties properties) {
        try {
            Class.forName(properties.getProperty(MetadataParams.META_EXTERN_SCHEMA_DRIVER).trim());
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Failed in building connection with external metadata storage", ex);
        }
        try {
            return DriverManager.getConnection(
                properties.getProperty(MetadataParams.META_EXTERN_SCHEMA_URL).trim(),
                properties.getProperty(MetadataParams.META_EXTERN_SCHEMA_USER).trim(),
                properties.getProperty(MetadataParams.META_EXTERN_SCHEMA_PASSWORD).trim()
            );
        } catch (SQLException ey) {
            ey.printStackTrace();
            throw new RuntimeException("Failed in building connection with external metadata storage", ey);
        }
    }

    /**
     * .
     */
    public static Properties readProperties(String fileName) {
        Properties properties = new Properties();
        String home = System.getenv("QSQL_HOME");
        File metaProperties;
        if (! StringUtils.isEmpty(home)) {
            metaProperties = new File(home + "/conf/" + fileName);
        } else {
            metaProperties = new File(MetadataUtil.class.getResource("/" + fileName).getFile());
        }
        try (InputStream input = new FileInputStream(metaProperties)) {
            properties.load(input);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return properties;
    }

    /**
     * .
     */
    public static String getMetadataFilePath() {
        return System.getenv("QSQL_HOME") + "/metastore/scripts";
    }
}
