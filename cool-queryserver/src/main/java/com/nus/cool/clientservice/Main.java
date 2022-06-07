package com.nus.cool.clientservice;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Properties;

public class Main {
    /**
     * Client package to send request to server. it will get broker's ip and send related execute request.
     * @param args query type,
     * @throws Exception Exception
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Pass in query id (Example: q1)");
            return;
        }

        CloseableHttpClient client = HttpClients.createDefault();
        String params;
        // todo, add more apis.
        if (args[0].equals("q1")) {
            params = "queryId=1&type=cohort";
        } else if (args[0].equals("q2")) {
            params = "queryId=2&type=iceberg";
        } else {
            System.err.println("Unrecognized query id");
            return;
        }
        String ip = "";
        try (InputStream input = new FileInputStream("conf/app.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            ip = prop.getProperty("server.host");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        String request = "http://" + ip + ":9013/broker/execute?" + params;
        URL url = new URL(request);
        URI uri = new URI(url.getProtocol(), null, url.getHost(), url.getPort(), url.getPath(), url.getQuery(), null);
        HttpGet get = new HttpGet(uri);
        client.execute(get);
    }
}