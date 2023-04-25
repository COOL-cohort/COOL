package com.nus.cool.queryserver.model;

/**
 * Record query's needed workerNum and startTime.
 */
public class QueryInfo {

  private int workNumber;

  private long startTime;

  public QueryInfo(int workNumber, long startTime) {
    this.workNumber = workNumber;
    this.startTime = startTime;
  }

  public int getWorkNumber() {
    return workNumber;
  }

  public void setWorkNumber(int workNumber) {
    this.workNumber = workNumber;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
}
