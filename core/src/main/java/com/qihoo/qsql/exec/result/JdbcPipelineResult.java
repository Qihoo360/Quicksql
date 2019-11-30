package com.qihoo.qsql.exec.result;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator of reading data from {@link JdbcPipelineResult}, which is the result of {@link
 * com.qihoo.qsql.exec.JdbcPipeline}.
 */
public abstract class JdbcPipelineResult implements PipelineResult {

    CloseableIterator<Object> iterator;

    private static final Logger LOGGER = LoggerFactory.getLogger(JobPipelineResult.class);

    JdbcPipelineResult(CloseableIterator<Object> iterator) {
        this.iterator = iterator;
    }

    @Override
    public ResultSet getData() {
        ResultSet resultSet = ((JdbcResultSetIterator) iterator).getResultSet();
        return resultSet;
    }

    void close() {
        try {
            iterator.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void run() {
    }

    public static class ShowPipelineResult extends JdbcPipelineResult {

        public ShowPipelineResult(CloseableIterator<Object> iterator) {
            super(iterator);
        }

        @Override
        public void run() {
            print();
            close();
        }

        /**
         * print result set.
         */
        public void print() {
            try {

                ResultSet resultSet = ((JdbcResultSetIterator) iterator).getResultSet();
                ResultSetMetaData meta = resultSet.getMetaData();
                int length = meta.getColumnCount();
                String[] colLabels = new String[length];
                int[] colCounts = new int[length];
                int[] types = new int[length];
                int[] changes = new int[length];
                Arrays.fill(changes, 0);

                for (int i = 0; i < meta.getColumnCount(); i++) {
                    colLabels[i] = meta.getColumnLabel(i + 1).toUpperCase();
                    types[i] = meta.getColumnType(i + 1);
                }

                fillWithDisplaySize(types, colCounts);
                printResults(meta, resultSet, colLabels, colCounts, changes);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        private void printResults(ResultSetMetaData meta, ResultSet resultSet,
            String[] colLabels, int[] colCounts, int[] changes) throws SQLException {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < meta.getColumnCount(); i++) {
                if (colLabels[i].length() > colCounts[i]) {
                    changes[i] = colLabels[i].length() - colCounts[i];
                    colCounts[i] = colLabels[i].length();
                }
                int sep = (colCounts[i] - colLabels[i].length());
                builder.append(String.format("|%s%" + (sep == 0 ? "" : sep) + "s", colLabels[i], ""));
            }
            builder.append("|");
            int[] colWeights = Arrays.copyOf(colCounts, colCounts.length);

            Function<String[], int[]> component = (labels) -> {
                int[] weights = new int[colWeights.length];
                for (int i = 0; i < weights.length; i++) {
                    weights[i] = colWeights[i] + changes[i];
                }
                return weights;
            };

            Supplier<String> framer = () ->
                "+" + Arrays.stream(component.apply(colLabels))
                    .mapToObj(col -> {
                        char[] fr = new char[col];
                        Arrays.fill(fr, '-');
                        return new String(fr);
                    }).reduce((x, y) -> x + "+" + y).orElse("") + "+";

            if (! resultSet.next()) {
                System.out.println("[Empty set]");
                return;
            }

            System.out.println(framer.get());
            System.out.println(builder.toString());
            System.out.println(framer.get());

            do {
                StringBuilder line = new StringBuilder();
                for (int i = 0; i < meta.getColumnCount(); i++) {
                    String value = resultSet.getString(i + 1);
                    if (value == null) {
                        value = "null";
                    }
                    if (value.length() > colCounts[i]) {
                        changes[i] = value.length() - colCounts[i];
                        colCounts[i] = value.length();
                    }
                    int sep = (colCounts[i] - value.length());
                    line.append(
                        String.format("|%s%" + (sep == 0 ? "" : sep) + "s", value, ""));
                }
                line.append("|");
                System.out.println(line.toString());
            }
            while (resultSet.next());

            System.out.println(framer.get());
        }

        private void fillWithDisplaySize(int[] type, int[] colCounts) {
            for (int i = 0; i < type.length; i++) {
                switch (type[i]) {
                    case Types.BOOLEAN:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                        colCounts[i] = 4;
                        break;
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.REAL:
                    case Types.FLOAT:
                    case Types.DOUBLE:
                        colCounts[i] = 8;
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        colCounts[i] = 20;
                        break;
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP:
                        colCounts[i] = 20;
                        break;
                    default:
                        colCounts[i] = 20;
                }
            }
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
