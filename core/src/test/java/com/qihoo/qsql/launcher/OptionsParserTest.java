package com.qihoo.qsql.launcher;

import com.qihoo.qsql.launcher.OptionsParser;
import com.qihoo.qsql.launcher.OptionsParser.SubmitOption;
import org.apache.commons.cli.ParseException;
import org.apache.xerces.impl.dv.util.Base64;
import org.junit.Assert;
import org.junit.Test;

public class OptionsParserTest {


    @Test
    public void testBasicFunction() throws ParseException {
        String[] args = {
            "--runner", "SPARK",
            "--master", "yarn-client",
            "--jar_name", "qsql-0.1.jar",
            "--jar", "tools.jar;sun.jar",
            "--sql", Base64.encode(("select * from department dep inner join "
            + "homework_content cont on dep.dep_id = cont.stu_id").getBytes()),
        };

        OptionsParser parser = new OptionsParser(args);
        Assert.assertEquals(parser.getOptionValue(SubmitOption.JAR_NAME), "qsql-0.1.jar");
        Assert.assertEquals(parser.getOptionValue(SubmitOption.MASTER_MODE), "yarn-client");
        Assert.assertEquals(parser.getOptionValue(SubmitOption.RUNNER), "SPARK");
        Assert.assertEquals(parser.getOptionValue(SubmitOption.JAR), "tools.jar;sun.jar");
    }

    @Test
    public void testErrorOptionLength() throws ParseException {
        String[] args = {
            "-runner", "SPARK",
            "-master", "yarn-client"
        };

        OptionsParser parser = new OptionsParser(args);
        Assert.assertEquals(parser.getOptionValue(SubmitOption.RUNNER), "SPARK");
        Assert.assertEquals(parser.getOptionValue(SubmitOption.MASTER_MODE), "yarn-client");
    }
}
