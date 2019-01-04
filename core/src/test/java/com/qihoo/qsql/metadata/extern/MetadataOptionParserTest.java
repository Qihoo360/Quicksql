package com.qihoo.qsql.metadata.extern;

import com.qihoo.qsql.exception.ParseException;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetadataOptionParserTest {

    MetadataOptionParser metadataOptionParser;

    @Before
    public void init() {
        metadataOptionParser = new MetadataOptionParser();
    }

    @Test
    public void testOptionParse() {

        List<String> args = Arrays.asList("--dbType=mysql", "--action=init");
        metadataOptionParser.parse(args);
        if (metadataOptionParser.getProperties().keySet().size() == 2) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testErrorDbType() {

        List<String> args = Arrays.asList("--dbType=oracle", "--action=init");
        try {
            metadataOptionParser.parse(args);
        } catch (ParseException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testErrorAction() {

        List<String> args = Arrays.asList("--dbType=mysql", "--action=initAndUpdate");
        try {
            metadataOptionParser.parse(args);
        } catch (ParseException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testEmptyAction() {

        List<String> args = Arrays.asList("--dbType=mysql", "--action=");
        try {
            metadataOptionParser.parse(args);
        } catch (ParseException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testEmptyDbType() {

        List<String> args = Arrays.asList("--dbType=", "--action=init");
        try {
            metadataOptionParser.parse(args);
        } catch (ParseException ex) {
            Assert.assertTrue(true);
        }
    }

}
