package com.nus.cool.queryserver.singleton;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nus.cool.queryserver.model.NodeInfo;
import com.nus.cool.queryserver.model.Worker;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ZKConnection {

  private static volatile ZKConnection instance = null;

  private static final int ZK_PORT = 2181;

  private ZooKeeper zk;

  public static ZKConnection getInstance() throws InterruptedException, IOException {
    if (instance == null) {
      synchronized (ZKConnection.class) {
        if (instance == null) {
          instance = new ZKConnection();
        }
      }
    }
    return instance;
  }

  private ZKConnection() throws InterruptedException, IOException {
    this.connect();
  }

  public void connect() throws IOException, InterruptedException {
    System.out.println("connect to zookeeper");
    String ZK_HOST = "";
    ModelConfig.getInstance();
    ZK_HOST = ModelConfig.props.getProperty("zookeeper.host");
    CountDownLatch latch = new CountDownLatch(1);
    zk = new ZooKeeper(ZK_HOST, ZK_PORT, watchedEvent -> {
        if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
            latch.countDown();
        }
    });
    latch.await();
  }

  /**
   * Add worker information to zookeeper
   *
   * @param serverHost worker address
   * @throws JsonProcessingException .
   * @throws KeeperException         .
   * @throws InterruptedException    .
   */
  public void addWorker(String serverHost)
      throws IOException, KeeperException, InterruptedException {
    NodeInfo info = new NodeInfo(serverHost, NodeInfo.Status.FREE);
    byte[] bytes = info.toByteArray();
    Stat workerExist = zk.exists("/workers", false);
    if (workerExist == null) {
      zk.create("/workers", "all workers".getBytes(StandardCharsets.UTF_8),
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    zk.create("/workers/", bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  public void createBroker(String serverHost) throws KeeperException, InterruptedException {
    zk.create("/broker", serverHost.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
  }

  public List<Worker> getWorkers() throws InterruptedException, KeeperException, IOException {
    List<Worker> workers = new ArrayList<>();
    List<String> workerNameList = zk.getChildren("/workers", false);
    for (String workerName : workerNameList) {
      NodeInfo info = this.getInfo("/workers/" + workerName);
      Worker worker = new Worker(workerName, info);
      workers.add(worker);
    }
    return workers;
  }

  public List<Worker> getFreeWorkers() throws InterruptedException, KeeperException, IOException {
    List<Worker> workers = this.getWorkers();
    List<Worker> freeWorkers = new ArrayList<>();
    for (Worker worker : workers) {
      NodeInfo info = worker.getInfo();
        if (info.getStatus() == NodeInfo.Status.FREE) {
            freeWorkers.add(worker);
        }
    }
    return freeWorkers;
  }

  public void allocateWorker(String workerName)
      throws IOException, KeeperException, InterruptedException {
    NodeInfo info = this.getInfo("/workers/" + workerName);
    info.setStatus(NodeInfo.Status.BUSY);
    this.zk.setData("/workers/" + workerName, info.toByteArray(), -1);
  }

  public void relaseWorker(String workerName)
      throws IOException, KeeperException, InterruptedException {
    NodeInfo info = this.getInfo("/workers/" + workerName);
    info.setStatus(NodeInfo.Status.FREE);
    this.zk.setData("/workers/" + workerName, info.toByteArray(), -1);
  }

  public NodeInfo getInfo(String path) throws KeeperException, InterruptedException, IOException {
    byte[] bytes = zk.getData(path, false, null);
    return NodeInfo.read(bytes);
  }

  public ZooKeeper getZK() {
    return this.zk;
  }

  public void close() throws InterruptedException {
    zk.close();
  }
}
