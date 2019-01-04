package com.qihoo.qsql.metadata.extern;

import com.qihoo.qsql.exception.QsqlException;
import java.util.Arrays;
import java.util.Properties;

/**
 * External metadata entry main class.
 * <p>
 * The default metadata storage of qsql is sqlite, which is also packed in tar with other classes. Otherwise, qsql also
 * provides a way to change this situation. To change some configuration in metadata.properties which is in /conf file
 * package, and execute command like "./metadata --dbType other_storage --action init", then you can reading and writing
 * metadata through other storage engine, e.g., mysql.
 * </p>
 */
public class MetadataMain {

    /**
     * Extern metadata entry main class.
     *
     * @param args options from command line
     */
    public static void main(String[] args) {
        MetadataTool tool = new MetadataTool();
        MetadataOptionParser parser = new MetadataOptionParser();
        parser.parse(Arrays.asList(args));
        Properties metadataProperties = parser.getProperties();
        try {
            tool.run(metadataProperties);
        } catch (Exception ex) {
            throw new QsqlException("Failed to process metadata", ex);
        }
    }

}
