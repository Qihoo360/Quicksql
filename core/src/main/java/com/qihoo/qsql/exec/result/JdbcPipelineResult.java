package com.qihoo.qsql.exec.result;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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
            try {
                ResultSetMetaData meta = ((JdbcResultSetIterator) iterator).getResultSet().getMetaData();
                String[] colLabels = new String[meta.getColumnCount()];
                for (int i = 0; i < meta.getColumnCount(); i++) {
                    colLabels[i] = meta.getColumnLabel(i + 1) + ":" + meta.getColumnTypeName(i + 1);
                }
                String metadata = Arrays.stream(colLabels).reduce((x, y) -> x + "," + y).orElse("");
                char[] sep = new char[metadata.length()];
                Arrays.fill(sep, '-');
                System.out.println(new String(sep) + "\n" + metadata + "\n" + new String(sep));
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
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
