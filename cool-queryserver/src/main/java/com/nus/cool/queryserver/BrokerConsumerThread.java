package com.nus.cool.queryserver;

import com.nus.cool.queryserver.model.Parameter;
import com.nus.cool.queryserver.model.QueryInfo;
import com.nus.cool.queryserver.model.Worker;
import com.nus.cool.queryserver.singleton.HDFSConnection;
import com.nus.cool.queryserver.singleton.QueryIndex;
import com.nus.cool.queryserver.singleton.TaskQueue;
import com.nus.cool.queryserver.singleton.WorkerIndex;
import com.nus.cool.queryserver.singleton.ZKConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;


/**
 * BrokerConsumerThread threads.
 */
public class BrokerConsumerThread extends Thread {

  /**
   * Run.zk.
   */
  public void run() {
    try {
      // 1. retrieve zk, task queue.
      System.out.println("thread running");
      ZKConnection zk = ZKConnection.getInstance();
      zk.getZK().create("/brokerThread", "running".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL);
      TaskQueue taskQueue = TaskQueue.getInstance();

      // 2. Consuming [ip, path] from taskQueue. generate get request, and sent to workers
      while (true) {
        try {
          TimeUnit.SECONDS.sleep(1);
          if (taskQueue.size() == 0) {
            continue;
          }
          List<Worker> workers = zk.getFreeWorkers();
          if (workers == null) {
            continue;
          }
          WorkerIndex workerIndex = WorkerIndex.getInstance();
          for (Worker worker : workers) {
            if (taskQueue.size() == 0) {
              break;
            }
            Parameter p = taskQueue.poll();
            workerIndex.put(worker.getWokerName(), p.getContent());

            // retrieve content and generate get request.
            // eg.  http://172.25.122.70:9011/dist/cohort?path=hdfs://localhost:9000/cube/health/v1/&file=1805b2fdb75.dz&queryId=1&worker=0000000003
            String req = "http://" + worker.getInfo().getIp() + "/" + p.getContent() + "&worker="
                + worker.getWokerName();

            System.out.println("Broker send get request = " + req);

            URL url = new URL(req);
            URI uri = new URI(url.getProtocol(), null, url.getHost(), url.getPort(), url.getPath(),
                url.getQuery(), null);
            HttpGet get = new HttpGet(uri);
            zk.allocateWorker(worker.getWokerName());
            CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
            client.start();
            client.execute(get, new FutureCallback<HttpResponse>() {
              @Override
              public void completed(HttpResponse httpResponse) {
                HttpEntity entity = httpResponse.getEntity();
                String queryId = null;
                try {
                  BufferedReader reader =
                      new BufferedReader(new InputStreamReader(entity.getContent()));
                  queryId = reader.readLine();
                  check(queryId);
                } catch (IOException | URISyntaxException e) {
                  System.out.println("error");
                }

                workerIndex.remove(worker.getWokerName());
                System.out.println(httpResponse.getStatusLine().getStatusCode());
                System.out.println("complete");
              }

              @Override
              public void failed(Exception e) {
                System.out.println("failed");
              }

              @Override
              public void cancelled() {
                System.out.println("cancelled");
              }
            });

            long st = System.currentTimeMillis();
            int index = p.getContent().indexOf("queryId=");
            String queryId = p.getContent().substring(index + 8, index + 9);
            System.out.println("waiting elapsed: "
                +
                (st - QueryIndex.getInstance().get(queryId).getStartTime()));
          }
        } catch (Exception e) {
          System.out.println(e.getMessage());
          break;
        }

      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Run server.
   *
   * @param queryId empty
   */
  public void check(String queryId) throws IOException, URISyntaxException {
    long end = System.currentTimeMillis();
    QueryIndex queryIndex = QueryIndex.getInstance();
    QueryInfo queryInfo = queryIndex.get(queryId);
    HDFSConnection fs = HDFSConnection.getInstance();
    int completedNumber = fs.getResults(queryId).length;
    if (completedNumber == queryInfo.getWorkNumber()) {
      System.out.println(queryId + " elapsed: " + (end - queryInfo.getStartTime()));
    }
  }
}
