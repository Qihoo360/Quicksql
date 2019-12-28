package com.qihoo.qsql.launcher;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Parse options from QSql command line.
 */
public class OptionsParser {

    private CommandLine commandLine;

    public OptionsParser(String[] args) throws ParseException {
        parseOptions(args);
    }

    private void parseOptions(String[] args) throws ParseException {
        SubmitOption[] values = SubmitOption.values();
        List<Option> optionList = Arrays.stream(values)
            .map(option ->
                Option.builder().longOpt(option.key).hasArg().build()).collect(Collectors.toList());
        Options options = new Options();
        optionList.forEach(options::addOption);
        CommandLineParser parser = new DefaultParser();
        commandLine = parser.parse(options, args);
    }

    /**
     * Get specific option.
     *
     * @param option Option which need to be query
     * @return Option value
     */
    public String getOptionValue(SubmitOption option) {
        String value;
        if (commandLine.hasOption(option.key)
            && ! (value = commandLine.getOptionValue(option.key)).isEmpty()) {
            return value;
        } else {
            if (option.value == null || option.value.isEmpty()) {
                throw new RuntimeException("Option " + option.key + " is indispensable."
                    + " Please complete options then retry to run.");
            }
            return option.value;
        }
    }

    public enum SubmitOption {
        CLASS_NAME("class_name", "", null, null),
        JAR_NAME("jar_name", "", "'non-opt'", null),
        JAR("jar", "", "jars", null),
        MASTER_MODE("master", "local[*]", "master", null),
        WORKER_MEMORY("worker_memory", "1G", "executor-memory", null),
        DRIVER_MEMORY("driver_memory", "3G", "driver-memory", null),
        WORKER_NUM("worker_num", "20", "num-executors", null),
        RUNNER("runner", "dynamic", null, null),
        SQL("sql", "", null, null),
        APP_NAME("app_name", "", null, null),
        FILE("file", "", null, null),
        PORT("port","5888",null,null);
        String key;
        String value;
        String sparkParam;
        String flinkParam;

        SubmitOption(String key, String value, String sparkParam, String flinkParam) {
            this.key = key;
            this.value = value;
            this.sparkParam = sparkParam;
            this.flinkParam = flinkParam;
        }
    }


}
