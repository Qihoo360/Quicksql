package com.qihoo.qsql.launcher;

import com.qihoo.qsql.exception.QsqlException;
import com.qihoo.qsql.exec.AbstractPipeline;
import com.qihoo.qsql.exec.Compilable;
import com.qihoo.qsql.exec.flink.FlinkPipeline;
import com.qihoo.qsql.exec.spark.SparkPipeline;
import com.qihoo.qsql.utils.PropertiesReader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Choose a calculation engine based on type of pipeline.
 * <p>
 * Different calculation engine means different arguments and different submit commands. {@link ProcessExecClient} will
 * choose a suitable ExecClient, generate related command and start a new process to execute on the engine chosen.
 * </p>
 */
public abstract class ProcessExecClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessExecClient.class);
    protected AbstractPipeline pipeline;
    protected OptionsParser parser;
    protected ArgumentsSupplier supplier;

    private ProcessExecClient(AbstractPipeline pipeline, OptionsParser parser) {
        this.pipeline = pipeline;
        this.parser = parser;
        supplier = new ArgumentsSupplier(parser);
    }

    /**
     * Create a client to start process.
     *
     * @param pipeline concrete class body generator
     * @return the specific client
     */
    public static ProcessExecClient createProcessClient(AbstractPipeline pipeline,
        OptionsParser parser) {
        if (pipeline instanceof SparkPipeline) {
            return new SparkExecClient(pipeline, parser);
        } else if (pipeline instanceof FlinkPipeline) {
            return new FlinkExecClient(pipeline, parser);
        } else {
            throw new QsqlException("Unsupported execution client!!");
        }
    }

    /**
     * Start a new process to execute.
     *
     * @return log or result output stream
     */
    public OutputStream exec() {
        CommandLine commandLine = CommandLine.parse(submit());
        commandLine.addArguments(arguments());
        LOGGER.debug("CommandLine is " + commandLine.toString());

        DefaultExecutor executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(System.out));
        try {
            executor.execute(commandLine);
        } catch (IOException ex) {
            LOGGER.error("Process executing failed!! Caused by: " + ex.getMessage());
            throw new RuntimeException(ex);
        }

        return System.out;
    }

    protected abstract String submit();

    protected abstract String[] arguments();

    protected String source() {
        if (! (pipeline instanceof Compilable)) {
            throw new RuntimeException("Can not get requirement source code from Pipeline");
        }
        return ((Compilable) pipeline).source();
    }

    protected String className() {
        return pipeline.getWrapper().getClassName();
    }

    public static class SparkExecClient extends ProcessExecClient {

        SparkExecClient(AbstractPipeline pipeline, OptionsParser parser) {
            super(pipeline, parser);
        }

        @Override
        protected String submit() {
            String sparkDir = System.getenv("SPARK_HOME");
            String sparkSubmit = sparkDir == null ? "spark-submit" : sparkDir
                + File.separator + "bin" + File.separator + "spark-submit";
            return PropertiesReader.isDevelopEnv() ? sparkSubmit + ".cmd" : sparkSubmit;
        }

        @Override
        protected String[] arguments() {
            List<String> args = supplier.assemblySparkOptions();
            args.add("--class_name");
            args.add(className());
            args.add("--source");
            args.add(new String(Base64.getEncoder().encode(source().getBytes()),
                StandardCharsets.UTF_8));
            return args.toArray(new String[0]);
        }
    }

    public static class FlinkExecClient extends ProcessExecClient {

        FlinkExecClient(AbstractPipeline pipeline, OptionsParser parser) {
            super(pipeline, parser);
        }

        @Override
        protected String submit() {
            String flinkDir = System.getenv("FLINK_HOME");
            return flinkDir == null ? "flink run" : flinkDir
                + File.separator + "bin" + File.separator + "flink run";
        }

        @Override
        protected String[] arguments() {
            return supplier.assemblyFlinkOptions();
        }
    }
}


