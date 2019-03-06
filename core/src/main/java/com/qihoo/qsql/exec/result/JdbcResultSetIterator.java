package com.qihoo.qsql.exec.result;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.RowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Iterator of reading data from ResultSet.
 */
public class JdbcResultSetIterator<T> extends LookaheadIterator<T> {

    public static final Function<Object, String> CONCAT_FUNC = (result) -> (String.join("\t",
        Arrays.stream((Object[]) result)
            .map(object -> object == null ? "null" : object.toString())
            .collect(Collectors.toList())));
    private static final Logger LOGGER = LoggerFactory.getLogger(
        JdbcResultSetIterator.class);
    private final ResultSet resultSet;
    private final RowProcessor convert;

    public JdbcResultSetIterator(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.convert = new BasicRowProcessor();
    }

    @Override
    protected T loadNext() {
        try {
            if (! resultSet.next()) {
                return null;
            }

            return (T) convert.toArray(resultSet);
        } catch (SQLException ex) {
            throw new IllegalStateException("Error reading from database", ex);
        }
    }

    @Override
    public void close() {

        Connection connection = null;
        Statement statement = null;
        if (resultSet != null) {
            try {
                statement = resultSet.getStatement();
                connection = statement.getConnection();
                resultSet.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
                LOGGER.error(ex.getMessage());
            }
        }

        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException ex) {
                LOGGER.error(ex.getMessage());
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ex) {
                LOGGER.error(ex.getMessage());
            }
        }
    }

    public ResultSet getResultSet() {
        return resultSet;
    }
}
