package com.qihoo.qsql.metadata.extern;

import com.qihoo.qsql.metadata.utils.MetadataUtil;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.ibatis.jdbc.ScriptRunner;

/**
 * Metadata tool for extern metadata.
 * <p>
 * Metadata tool will create connection first, then find the sql script based on params. Finally, it would execute sql
 * script and do actions for external storage.
 * </p>
 */
public class MetadataTool {

    private static Properties properties = new Properties();

    static {
        properties = MetadataUtil.readProperties("metadata.properties");
    }

    /**
     * Find sql file based on command line options. Then execute sql file in external storage.
     *
     * @param metadataProperties options parsed from command line
     */
    public void run(Properties metadataProperties) {
        if (MetadataUtil.isEmbeddedDatabase(properties)) {
            throw new RuntimeException("Extern metadata storage config is not set properly! "
                + "Please Check metadata.properties.");
        }

        if (! metadataProperties.containsKey("--dbType")) {
            throw new RuntimeException("Please set --dbType");
        }

        String fileName = "qsql-metadata-"
            + metadataProperties.getProperty("--dbType").toLowerCase()
            + "-"
            + metadataProperties.getProperty("--action").toLowerCase()
            + ".sql";

        try (Connection connection = MetadataUtil.getExternalConnection(properties)) {
            ScriptRunner runner = new ScriptRunner(connection);
            runner.setErrorLogWriter(null);
            runner.setLogWriter(null);
            try {
                runner.runScript(
                    new FileReader(MetadataUtil.getMetadataFilePath() + File.separator + fileName)
                );
            } catch (IOException ex) {
                throw new RuntimeException("Failed in reading sql line from : " + fileName, ex);
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Failed in close connection with extern metadata storage", ex);
        }
    }

}
