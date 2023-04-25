package com.nus.cool.queryserver.model;

/**
 * worker.
 */
public class Worker {

  private String workerName;

  private NodeInfo info;

  public Worker(String workerName, NodeInfo info) {
    this.workerName = workerName;
    this.info = info;
  }

  public String getWokerName() {
    return workerName;
  }

  public void setWokerName(String wokerName) {
    this.workerName = wokerName;
  }

  public NodeInfo getInfo() {
    return info;
  }

  public void setInfo(NodeInfo info) {
    this.info = info;
  }
}
