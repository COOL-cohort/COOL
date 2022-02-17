package com.nus.cool.queryserver;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

public class QueryServer {
    public static void main(String[] args) throws Exception{
        Server server = new Server(8080);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        // 配置Servlet
        ServletHolder holder = context.addServlet(ServletContainer.class, "/*");
//        holder.setInitOrder(1);
        holder.setInitParameter("jersey.config.server.provider.packages", "com.nus.cool.queryserver");

        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            server.destroy();
        }

    }
}