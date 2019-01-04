package com.qihoo.qsql.metadata.extern;

import com.qihoo.qsql.exception.ParseException;
import com.qihoo.qsql.exception.QsqlException;
import com.qihoo.qsql.metadata.utils.MetaConnectionUtil;
import com.qihoo.qsql.utils.PropertiesReader;
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

    private static Properties properties;

    static {
        properties = PropertiesReader.readProperties("metadata.properties");
    }

    /**
     * Find sql file based on command line options. Then execute sql file in external storage.
     *
     * @param metadataProperties options parsed from command line
     */
    public void run(Properties metadataProperties) {
        if (MetaConnectionUtil.isEmbeddedDatabase(properties)) {
            throw new QsqlException("Extern metadata storage config is not set properly! "
                + "Please Check metadata.properties.");
        }

        if (! metadataProperties.containsKey("--dbType")) {
            throw new ParseException("Please set --dbType");
        }

        String fileName = "qsql-metadata-"
            + metadataProperties.getProperty("--dbType").toLowerCase()
            + "-"
            + metadataProperties.getProperty("--action").toLowerCase()
            + ".sql";

        Connection connection = MetaConnectionUtil.getExternalConnection(properties);
        ScriptRunner runner = new ScriptRunner(connection);
        runner.setErrorLogWriter(null);
        runner.setLogWriter(null);
        try {
            runner.runScript(
                new FileReader(PropertiesReader.getMetadataFilePath() + File.separator + fileName)
            );
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new QsqlException("Failed in reading sql line from : " + fileName, ex);
        } finally {
            try {
                connection.close();
            } catch (SQLException ey) {
                throw new QsqlException("Failed in close connection with extern metadata storage");
            }
        }
    }

}
