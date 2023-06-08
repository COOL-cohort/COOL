package com.nus.cool.queryserver.singleton;

import com.nus.cool.queryserver.model.QueryInfo;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Initialize QueryIndex, record query information.
 */
public class QueryIndex {

  private final Map<String, QueryInfo> index = new ConcurrentHashMap<>();

  // keep QueryIndex in memory, preventing thread copying it to local register.
  private static volatile QueryIndex instance = null;

  /**
   * getInstance.
   */
  // Thread Safe Singleton
  public static QueryIndex getInstance() {
    if (instance == null) {
      synchronized (QueryIndex.class) {
        if (instance == null) {
          instance = new QueryIndex();
        }
      }
    }
    return instance;
  }

  // prevent it to be initialized outside
  private QueryIndex() {
  }

  public void put(String queryId, QueryInfo queryInfo) {
    this.index.put(queryId, queryInfo);
  }

  public QueryInfo get(String queryId) {
    return this.index.get(queryId);
  }
}
