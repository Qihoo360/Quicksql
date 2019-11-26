package com.qihoo.qsql.exec.result;

import com.qihoo.qsql.exec.Requirement;
import java.util.Collection;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator of reading data from PipelineResult which is created by non-jdbc task, such as Spark.
 */
public abstract class JobPipelineResult implements PipelineResult {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobPipelineResult.class);

    private String path;
    private boolean persistOrNot = false;
    private Requirement requirement;

    JobPipelineResult(String path, Requirement requirement) {
        this.path = path;
        this.requirement = requirement;
    }

    JobPipelineResult(Requirement requirement) {
        this.path = "";
        this.requirement = requirement;
    }

    @Override
    public Collection<String> getData() {
        return Collections.emptyList();
    }

    @Override
    public void run() {
        LOGGER.debug("Start executing Requirement!!");
        try {
            requirement.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            requirement.close();
        }
    }

    public static class JsonPipelineResult extends JobPipelineResult {

        public JsonPipelineResult(String path, Requirement requirement) {
            super(requirement);
        }

        @Override
        public Collection<String> getData() {
            return Collections.emptyList();
        }
    }

    public static class TextPipelineResult extends JobPipelineResult {

        private String deliminator;

        public TextPipelineResult(String path, String deliminator, Requirement requirement) {
            super(path, requirement);
            this.deliminator = deliminator;
        }

        @Override
        public Collection<String> getData() {
            return Collections.emptyList();
        }
    }

    public static class ShowPipelineResult extends JobPipelineResult {

        public ShowPipelineResult(Requirement requirement) {
            super(requirement);
        }

        @Override
        public Collection<String> getData() {
            return Collections.emptyList();
        }
    }
}
