package com.nus.cool.queryserver.handler;

import com.nus.cool.queryserver.model.Parameter;
import com.nus.cool.queryserver.model.QueryInfo;
import com.nus.cool.queryserver.singleton.HDFSConnection;
import com.nus.cool.queryserver.singleton.QueryIndex;
import com.nus.cool.queryserver.singleton.TaskQueue;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * BrokerController.
 */
@RestController
@RequestMapping("/broker")
public class BrokerController {

  /**
   * loadToDfs.
   */
  @PostMapping(value = "/load-dfs")
  public ResponseEntity<String> loadToDfs() {
    return null;
  }

  /**
   * execute.
   */
  @GetMapping(value = "/execute")
  public ResponseEntity<String> handler(@RequestParam Map<String, String> params) {
    System.out.println(params);
    try {

      // 1. retrieve queryID and type
      String queryId = params.get("queryId");
      String queryType = params.get("type");

      // 2. connect to hdfs, get data Source Name, cohort or iceberg
      HDFSConnection fs = HDFSConnection.getInstance();
      String source = queryType.equals("cohort") ? fs.readCohortQuery(queryId).getDataSource()
          : fs.readIcebergQuery(queryId).getDataSource();

      // generate query
      List<String> parameters = fs.getParameters(source, queryId, queryType);

      // 3. record query's workerNum, startTime in queryIndex
      long begin = System.currentTimeMillis();
      QueryInfo queryInfo = new QueryInfo(parameters.size(), begin);
      QueryIndex queryIndex = QueryIndex.getInstance();
      queryIndex.put(queryId, queryInfo);

      // 4. produce task to the TaskQueue
      TaskQueue taskQueue = TaskQueue.getInstance();
      for (String content : parameters) {
        Parameter p = new Parameter(1, content);
        taskQueue.add(p);
      }
      return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body("Done");
    } catch (Exception e) {
      e.printStackTrace();
      return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body(e.getMessage());
    }
  }
}

