package com.qihoo.qsql.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.PropertyConfigurator;

public class PropertiesReader {

    /**
     * Read properties file.
     *
     * @param fileName fileName in conf
     * @return Properties
     */
    public static Properties readProperties(String fileName) {
        Properties properties = new Properties();
        File confParentDir = new File(getConfFilePath());

        try (InputStream input = new FileInputStream(
            new File(confParentDir.getAbsolutePath() + "/" + fileName))) {
            properties.load(input);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return properties;
    }

    /**
     * Create conf file path.
     *
     * @return conf file path
     */
    public static String getConfFilePath() {
        if (isDevelopEnv()) {
            return System.getProperty("user.dir") + "/../conf";
        } else {
            return System.getenv("QSQL_HOME") + "/conf";
        }
    }

    /**
     * Create metadata file path.
     *
     * @return metadata file path
     */
    public static String getMetadataFilePath() {
        if (isDevelopEnv()) {
            return System.getProperty("user.dir") + "/../metastore/scripts";
        } else {
            return System.getenv("QSQL_HOME") + "/metastore/scripts";
        }
    }

    /**
     * Create test data file path.
     *
     * @return test data file path
     */
    public static String getTestDataFilePath() {
        if (isDevelopEnv()) {
            return (System.getProperty("user.dir") + "/data/sales/DEPTS.csv").replace("\\", "/");
        } else {
            return System.getenv("QSQL_HOME") + "/data/sales/DEPTS.csv";
        }
    }

    /**
     * whether to execute in development environment.
     */
    public static boolean isDevelopEnv() {
        String osName = System.getProperties().getProperty("os.name");
        return osName.contains("Windows") || osName.contains("Mac");
    }

    /**
     * Used to distinguish script execution methods in different environments.
     */
    public static boolean isSupportedShell() {
        String osName = System.getProperties().getProperty("os.name");
        return ! osName.contains("Windows");
    }

    /**
     * Read log properties.
     */
    public static void configLogger() {
        String logProp;
        if (((logProp = System.getenv("QSQL_HOME")) != null) && ! logProp.isEmpty()) {
            PropertyConfigurator.configure(logProp
                + File.separator + "conf" + File.separator + "log4j.properties");
        }
    }
}
