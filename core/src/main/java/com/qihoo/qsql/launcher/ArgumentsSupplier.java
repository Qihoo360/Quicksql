package com.qihoo.qsql.launcher;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.launcher.OptionsParser.SubmitOption;
import com.qihoo.qsql.utils.PropertiesReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Generate Spark execution command.
 */
public class ArgumentsSupplier {

    public OptionsParser parser;

    SqlRunner.Builder builder;

    public ArgumentsSupplier(OptionsParser parser, SqlRunner.Builder builder) {
        this.parser = parser;
        this.builder = builder;
    }

    /**
     * Assemble spark options.
     *
     * @return options
     */
    public List<String> assemblySparkOptions() {
        List<String> arguments = new ArrayList<>();
        Arrays.stream(OptionsParser.SubmitOption.values())
            .filter(submission -> submission.sparkParam != null
                && !submission.sparkParam.equals("'non-opt'"))
            .forEach(submission -> {
                arguments.add(longSparkOpt(submission));
                arguments.add(parser.getOptionValue(submission));
            });

        List<String> conf = loadSparkConf();
        conf.forEach(attr -> {
            arguments.add(longSparkOpt("conf"));
            arguments.add(attr);
        });


        arguments.add(longSparkOpt("class"));
        arguments.add(ProcessExecutor.class.getCanonicalName());
        arguments.add(parser.getOptionValue(OptionsParser.SubmitOption.JAR_NAME));
        arguments.add(longSparkOpt("jar"));
        arguments.add(parser.getOptionValue(SubmitOption.JAR));
        arguments.add(longSparkOpt("master"));
        arguments.add(parser.getOptionValue(SubmitOption.MASTER_MODE));
        arguments.add(longSparkOpt("runner"));
        arguments.add(parser.getOptionValue(OptionsParser.SubmitOption.RUNNER));
        return arguments;
    }

    private List<String> loadSparkConf() {
        Properties properties =
            PropertiesReader.readProperties("quicksql-runner.properties", this.getClass());
        //only mongo query job need set 'spark.mongodb.input.uri' parameter.
        if (builder.getRunnerProperties().size() > 0 && builder.getRunnerProperties().getProperty("dbType")
            .equalsIgnoreCase("mongo")) {
            properties.put("spark.mongodb.input.uri", constructMongoUrl(builder.getRunnerProperties()));
        }
        return properties.entrySet().stream()
            .filter(conf -> conf.getKey().toString().startsWith("spark"))
            .map(conf -> conf.getKey() + "=" + conf.getValue())
            .collect(Collectors.toList());
    }

    private String longSparkOpt(OptionsParser.SubmitOption option) {
        return "--" + option.sparkParam;
    }

    private String longSparkOpt(String attr) {
        return "--" + attr;
    }

    /**
     * Assemble flink options.
     *
     * @return options
     */
    public List<String> assemblyFlinkOptions() {
        List<String> arguments = new ArrayList<>();
        Arrays.stream(OptionsParser.SubmitOption.values())
            .filter(submission -> submission.flinkParam != null
                && !submission.flinkParam.equals("'non-opt'"))
            .forEach(submission -> {
                arguments.add(longSparkOpt(submission));
                arguments.add(parser.getOptionValue(submission));
            });

        // List<String> conf = loadSparkConf();
        // conf.forEach(attr -> {
        //     arguments.add(longSparkOpt("conf"));
        //     arguments.add(attr);
        // });


        arguments.add(longSparkOpt("class"));
        arguments.add(ProcessExecutor.class.getCanonicalName());
        arguments.add(parser.getOptionValue(OptionsParser.SubmitOption.JAR_NAME));
        arguments.add(longSparkOpt("jar"));
        arguments.add(parser.getOptionValue(SubmitOption.JAR));
        arguments.add(longSparkOpt("master"));
        arguments.add(parser.getOptionValue(SubmitOption.MASTER_MODE));
        arguments.add(longSparkOpt("runner"));
        arguments.add(parser.getOptionValue(OptionsParser.SubmitOption.RUNNER));
        return arguments;
    }


    protected String constructMongoUrl(Properties properties) {
        //mongodb url like "mongodb://user:pass@localhost:27017/dbName.collectionName")
        StringBuilder mongoUrl = new StringBuilder();
        mongoUrl.append("mongodb://")
            .append(StringUtils.isNotEmpty(properties.getProperty("userName")) ? properties.getProperty("userName")
                + ":" : "")
            .append(StringUtils.isNotEmpty(properties.getProperty("password"))
                ? properties.getProperty("password") : "")
            .append("@" + properties.getProperty("host"))
            .append(":" + properties.getProperty("port"))
            .append("/" + properties.getProperty("dbName"))
            .append("." + properties.getProperty("collectionName"));
        return mongoUrl.toString();
    }
}
