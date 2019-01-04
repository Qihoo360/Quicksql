package com.qihoo.qsql.launcher;

import com.github.picadoh.imc.compiler.InMemoryCompiler.CompilerException;
import com.qihoo.qsql.codegen.ClassBodyWrapper;
import com.qihoo.qsql.exec.flink.FlinkRequirement;
import com.qihoo.qsql.exec.spark.SparkRequirement;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Base64;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.spark.sql.SparkSession;

/**
 * Entry Class of the second submit, based on the Java code generated and arguments.
 */
public class ProcessExecutor {

    /**
     * Execute program.
     *
     * @param args arguments
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new RuntimeException("Need to given a Requirement class hand its class name!");
        }

        Option optionSourceCode = Option.builder().longOpt("source").hasArg().desc("source code").build();
        Option optionClassName = Option.builder().longOpt("class_name").hasArg().desc("class Name").build();
        Option optionJars = Option.builder().longOpt("jar").hasArg().desc("jars").build();
        Option optionAdapter = Option.builder().longOpt("runner").hasArg().desc("compute runner type").build();
        Option optionAppName = Option.builder().longOpt("app_name").hasArg().desc("app name").build();

        Options options = new Options();
        options.addOption(optionSourceCode).addOption(optionClassName)
            .addOption(optionAdapter).addOption(optionAppName).addOption(optionJars);
        CommandLineParser parser = new DefaultParser();

        String className;
        String source;
        String runner;
        String appName = "QSQL-" + UUID.randomUUID();
        String extraJars;

        try {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption("app_name")) {
                appName = commandLine.getOptionValue("app_name");
            }
            if (commandLine.hasOption("source")
                && commandLine.hasOption("class_name")
                && commandLine.hasOption("runner")
                && commandLine.hasOption("jar")) {
                source = new String(Base64.getDecoder().decode(commandLine.getOptionValue("source")));
                className = commandLine.getOptionValue("class_name");
                runner = commandLine.getOptionValue("runner");
                extraJars = commandLine.getOptionValue("jar");
            } else {
                throw new RuntimeException("Options --source or --className or --runner not found");
            }
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }

        ProcessExecutor executor = new ProcessExecutor();
        executor.execute(source, className, runner, appName, extraJars);
    }

    @SuppressWarnings("unchecked")
    private void execute(String source, String className, String runner, String appName, String extraJars) {
        Class requirementClass;
        try {
            requirementClass = ClassBodyWrapper.compileSourceAndLoadClass(
                source, className, extraJars.replaceAll(",", System.getProperty("path.separator")));
        } catch (CompilerException | ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }

        switch (runner.toUpperCase()) {
            case "DYNAMIC":
            case "SPARK":
                try {
                    final Constructor<SparkRequirement> constructor =
                        ((Class<SparkRequirement>) requirementClass).getConstructor(SparkSession.class);
                    SparkSession sc = SparkSession.builder()
                        .appName(appName)
                        .enableHiveSupport()
                        .getOrCreate();

                    constructor.newInstance(sc).execute();
                    sc.stop();
                } catch (NoSuchMethodException | IllegalAccessException
                    | InvocationTargetException | InstantiationException ex) {
                    throw new RuntimeException(ex);
                }
                break;
            case "FLINK":
                try {
                    final Constructor<FlinkRequirement> constructor =
                        ((Class<FlinkRequirement>) requirementClass).getConstructor(ExecutionEnvironment.class);

                    ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
                    constructor.newInstance(executionEnvironment).execute();
                } catch (NoSuchMethodException | IllegalAccessException
                    | InvocationTargetException | InstantiationException ex) {
                    throw new RuntimeException(ex);
                }
                break;
            default:
        }
    }
}
