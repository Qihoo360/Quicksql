package com.qihoo.qsql.metadata.utils;

import com.qihoo.qsql.exception.QsqlException;
import com.qihoo.qsql.metadata.MetadataParams;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Utils for make connection with extern storage.
 */
public class MetaConnectionUtil {

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
            throw new QsqlException("Failed in building connection with external metadata storage", ex);
        }
        try {
            return DriverManager.getConnection(
                properties.getProperty(MetadataParams.META_EXTERN_SCHEMA_URL).trim(),
                properties.getProperty(MetadataParams.META_EXTERN_SCHEMA_USER).trim(),
                properties.getProperty(MetadataParams.META_EXTERN_SCHEMA_PASSWORD).trim()
            );
        } catch (SQLException ey) {
            ey.printStackTrace();
            throw new QsqlException("Failed in building connection with external metadata storage", ey);
        }
    }
}
