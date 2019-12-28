package com.qihoo.qsql.server;

import com.qihoo.qsql.client.Driver;
import com.qihoo.qsql.launcher.OptionsParser;
import com.qihoo.qsql.launcher.OptionsParser.SubmitOption;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.List;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;
import org.apache.calcite.avatica.server.Main.HandlerFactory;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class JdbcServer {

    public static void main(String[] args) throws Exception {
        OptionsParser parser = new OptionsParser(args);
        final int port = Integer.parseInt(parser.getOptionValue(SubmitOption.PORT));
        final String[] mainArgs = new String[]{FullyRemoteJdbcMetaFactory.class.getName()};

        HttpServer jsonServer = Main.start(mainArgs, port, new HandlerFactory() {
            @Override
            public AbstractHandler createHandler(Service service) {
                return new AvaticaJsonHandler(service);
            }
        });
        InetAddress address = InetAddress.getLocalHost();
        String hostName = "localhost";
        if (address != null) {
             hostName = StringUtils.isNotBlank(address.getHostName()) ? address.getHostName() : address
                .getHostAddress();
        }
        String url = Driver.CONNECT_STRING_PREFIX + "url=http://" + hostName + ":" + jsonServer.getPort();
        System.out.println("Quicksql server started, Please connect : " + url);
        jsonServer.join();
    }
    public static class FullyRemoteJdbcMetaFactory implements Meta.Factory {

        private static QuicksqlServerMeta instance = null;

        private static QuicksqlServerMeta getInstance() {
            if (instance == null) {
                try {
                    Class.forName("com.qihoo.qsql.server.Driver");
                    instance = new QuicksqlServerMeta("jdbc:quicksql:server:");
                }  catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            return instance;
        }

        @Override
        public Meta create(List<String> args) {
            return getInstance();
        }
    }

}
