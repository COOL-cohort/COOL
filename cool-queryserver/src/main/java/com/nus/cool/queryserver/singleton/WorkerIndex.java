package com.nus.cool.queryserver.singleton;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * record each worker's assigned tasks.
 */
public class WorkerIndex {

  // < workerName: url contents >
  private final Map<String, String> index = new ConcurrentHashMap<>();

  private static volatile WorkerIndex instance = null;

  private WorkerIndex() {
  }

  /**
   * WorkerIndex singleton.
   */
  public static WorkerIndex getInstance() {
    if (instance == null) {
      synchronized (WorkerIndex.class) {
        if (instance == null) {
          instance = new WorkerIndex();
        }
      }
    }
    return instance;
  }

  public void put(String worker, String parameter) {
    this.index.put(worker, parameter);
  }

  /**
   * Get all url contents of the give worker list.
   *
   * @param workers give worker list
   * @return all url contents assigned to those worker.
   */
  public List<String> checkDisconnected(List<String> workers) {
    List<String> parameters = new ArrayList<>();
    for (Map.Entry<String, String> entry : index.entrySet()) {
      if (!workers.contains(entry.getKey())) {
        parameters.add(entry.getValue());
        parameters.remove(entry.getKey());
      }
    }
    return parameters;
  }

  /**
   * remove worker.
   */
  public void remove(String worker) {
    this.index.remove(worker);
  }

  /**
   * To string.
   */
  public String toString() {
    String content = null;
    try {
      content = new ObjectMapper().writeValueAsString(this.index);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return content;
  }
}
