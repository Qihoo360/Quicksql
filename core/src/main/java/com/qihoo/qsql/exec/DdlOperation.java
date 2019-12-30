package com.qihoo.qsql.exec;

import java.sql.SQLException;

public interface DdlOperation {
    /**
     * execute the sql.
     */
    void execute(String sql) throws SQLException;
}
