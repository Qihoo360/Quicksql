package com.qihoo.qsql.exec.result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Iterator of reading data from {@link JdbcPipelineResult}, which is the result of {@link
 * com.qihoo.qsql.exec.JdbcPipeline}.
 */
public abstract class JdbcPipelineResult implements PipelineResult {

    CloseableIterator<Object> iterator;

    JdbcPipelineResult(CloseableIterator<Object> iterator) {
        this.iterator = iterator;
    }

    @Override
    public Collection<String> getData() {
        List<String> results = new ArrayList<>();
        iterator.forEachRemaining(JdbcResultSetIterator.CONCAT_FUNC::apply);
        close();
        return results;
    }

    void close() {
        try {
            iterator.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class ShowPipelineResult extends JdbcPipelineResult {

        public ShowPipelineResult(CloseableIterator<Object> iterator) {
            super(iterator);
        }

        @Override
        public void run() {
            if (! iterator.hasNext()) {
                System.out.println("Empty set");
            }
            iterator.forEachRemaining(result -> System.out.println(JdbcResultSetIterator.CONCAT_FUNC.apply(result)));
            close();
        }
    }

    /**
     * TextPipelineResult.
     */
    public static class TextPipelineResult extends JdbcPipelineResult {

        private String deliminator;
        private String path;

        /**
         * TextPipelineResult constructor.
         *
         * @param iterator iterator of result
         * @param path path of result
         * @param deliminator deliminator of result
         */
        public TextPipelineResult(CloseableIterator<Object> iterator, String path,
            String deliminator) {
            super(iterator);
            this.path = path;
            this.deliminator = deliminator;
        }

        @Override
        public void run() {
            //NOT IMPLEMENT
            super.iterator.forEachRemaining(result -> System.out.println(result.toString()));
            close();
        }
    }

    public static class JsonPipelineResult extends JdbcPipelineResult {

        private String path;

        public JsonPipelineResult(CloseableIterator<Object> iterator, String path) {
            super(iterator);
            this.path = path;
        }

        @Override
        public void run() {
            //NOT IMPLEMENT
            super.iterator.forEachRemaining(result -> System.out.println(result.toString()));
            close();
        }
    }
}
