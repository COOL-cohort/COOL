package com.nus.cool.queryserver;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jetty.JettyHttpContainer;
import org.glassfish.jersey.server.ContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jetty9.InstrumentedConnectionFactory;
import com.codahale.metrics.jetty9.InstrumentedHandler;
import com.codahale.metrics.jetty9.InstrumentedQueuedThreadPool;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueryServer implements Runnable {

    private Thread thisThread;

    private AtomicBoolean started = new AtomicBoolean(false);

    private CountDownLatch waitForLatch = new CountDownLatch(1);

    private volatile boolean bStop = false;

    private QueryServerModel model;

    private String datasetPath = "test/";

    public QueryServer(){}

    @Override
    public void run() {
        System.out.println("[*] Start the Query Server...");
        try {
            this.model = new QueryServerModel(this.datasetPath);
            Server httpServer = createJettyServer(8080, 100, new QueryServerController(this.model));
            httpServer.start();
            System.out.println(httpServer);
            httpServer.join();
//            while (!httpServer.isRunning())
//                Thread.sleep(100);
//            waitForLatch.countDown();
//
//            // mainLoop
//            while (!bStop)
//                Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start() throws Exception {
        if (!started.getAndSet(true)) {
            thisThread = new Thread(this);
            thisThread.start();
        }
    }

    public void waitForStart() throws InterruptedException {
        waitForLatch.await();
    }

    public void join() throws InterruptedException {
        if (started.get()) {
            thisThread.join();
        }
    }

    private Server createJettyServer(int port, int poolSize, Object controller) {
        MetricRegistry jettyMetrics = new MetricRegistry();
        InstrumentedQueuedThreadPool pool = new InstrumentedQueuedThreadPool(
                jettyMetrics, poolSize);

        Server server = new Server(pool);
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        ResourceConfig restConfig = new ResourceConfig();
        // restConfig.registerClasses(QueryServerController.class);
        restConfig.registerInstances(controller);
        restConfig.register(JacksonFeature.class);

        Handler handler = ContainerFactory.createContainer(
                JettyHttpContainer.class, restConfig);
        InstrumentedHandler metricsHandler = new InstrumentedHandler(jettyMetrics);
        metricsHandler.setHandler(handler);

        server.setConnectors(new Connector[]{connector});
        server.setHandler(metricsHandler);
        return server;
    }


//    public static void main(String[] args) throws Exception{
//        Server server = new Server(8080);
//
//        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
//        context.setContextPath("/");
//        server.setHandler(context);
//
//        // 配置Servlet
//        ServletHolder holder = context.addServlet(ServletContainer.class, "/*");
//        holder.setInitOrder(1);
//        holder.setInitParameter("jersey.config.server.provider.packages", "com.nus.cool.queryserver");
//
//        try {
//            server.start();
//            server.join();
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            server.destroy();
//        }
//    }

    public static void main(String[] args) throws Exception {
        QueryServer qserver = new QueryServer();
        qserver.start();
        qserver.waitForStart();
        qserver.join();
    }
}