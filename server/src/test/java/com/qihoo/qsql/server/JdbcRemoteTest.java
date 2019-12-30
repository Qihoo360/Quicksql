package com.qihoo.qsql.server;

import com.qihoo.qsql.client.Driver;
import com.qihoo.qsql.server.JdbcServer.FullyRemoteJdbcMetaFactory;
import java.lang.reflect.InvocationTargetException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JdbcRemoteTest {

    private HttpServer jsonServer;

    @Before
    public void testJdbcServer()
        throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, InterruptedException {
        final String[] mainArgs = new String[]{FullyRemoteJdbcMetaFactory.class.getName()};
        jsonServer = Main.start(mainArgs, 5888, AvaticaJsonHandler::new);
        String url = Driver.CONNECT_STRING_PREFIX + "url=http://localhost:" + jsonServer.getPort();
        System.out.println(url);
        new Thread(() -> {
            try {
                jsonServer.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @After
    public void shutdown() {
        jsonServer.stop();
    }

    @Test
    public void testExecuteQuery() {
        try {
            AvaticaConnection conn = getConnection();
            final AvaticaStatement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("select * from (values (1, 'a'), (2, 'b'))");
            while (rs.next()) {
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
            }
            close(rs, statement,conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPrepareStatementQuery() {
        try {
            AvaticaConnection conn = getConnection();
            final AvaticaStatement statement = conn.createStatement();
            PreparedStatement preparedStatement = conn.prepareStatement("select * from (values (1, 'a'), (2, 'b'), "
                + "(3, 'c')) "
                + "where expr_col__0 = ? or expr_col__1 = ?");
            preparedStatement.setInt(1,1);
            preparedStatement.setString(2,"b");
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
            }
            close(rs, statement,conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQueryMetaData() {
        try {
            AvaticaConnection conn = getConnection();
            final AvaticaStatement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("select * from (values (1, 'a'), (2, 'b'))");
            if (rs.getMetaData() != null) {
                System.out.println(rs.getMetaData().getColumnCount());
                System.out.println(rs.getMetaData().getColumnName(1));
                System.out.println(rs.getMetaData().getColumnName(2));
            }
            close(rs, statement,conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInsertQuery() {
        try {
            AvaticaConnection conn = getConnection();
            final AvaticaStatement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("INSERT INTO `hdfs://cluster:9000/` IN HDFS SELECT 1");
            while (rs.next()) {
                System.out.println(rs.getInt(1));
            }
            close(rs, statement,conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private AvaticaConnection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.qihoo.qsql.client.Driver");
        String url = "jdbc:quicksql:url=http://localhost:5888";
        Properties properties = new Properties();
        properties.put("runner","jdbc");
        return (AvaticaConnection) DriverManager.getConnection(url,properties);
    }

    private void close( ResultSet rs, AvaticaStatement statement,AvaticaConnection conn) throws SQLException {
        rs.close();
        statement.close();
        conn.close();
    }
}
