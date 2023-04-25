package com.nus.cool.queryserver.singleton;

import com.nus.cool.queryserver.model.Parameter;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Controller assign task into queue.
 */
public class TaskQueue {

  private final Queue<Parameter> queue = new PriorityBlockingQueue<>();

  private static volatile TaskQueue instance = null;

  private TaskQueue() {
  }

  /**
   * getInstance.
   */
  public static TaskQueue getInstance() {
    if (instance == null) {
      synchronized (TaskQueue.class) {
        if (instance == null) {
          instance = new TaskQueue();
        }
      }
    }
    return instance;
  }

  public void add(Parameter p) {
    this.queue.add(p);
  }

  public Parameter poll() {
    return this.queue.poll();
  }

  public int size() {
    return this.queue.size();
  }
}
