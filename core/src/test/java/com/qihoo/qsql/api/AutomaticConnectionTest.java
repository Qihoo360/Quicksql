package com.qihoo.qsql.api;

import org.junit.Assert;
import org.junit.Test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AutomaticConnectionTest {

    @Test
    public void testBasicConnectFunction() {
        try {
            Connection connection = SqlRunner.getConnection();
            PreparedStatement statement = connection.prepareStatement("select 1");
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            Assert.assertEquals(resultSet.getInt(1), 1);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
