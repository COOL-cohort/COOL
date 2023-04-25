package com.nus.cool.queryserver.handler;

import static com.nus.cool.functionality.CohortAnalysis.performCohortAnalysis;
import static com.nus.cool.functionality.OLAPAnalysis.performOLAPAnalysis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.CohortQueryLayout;
import com.nus.cool.core.cohort.OLAPQueryLayout;
import com.nus.cool.core.cohort.storage.CohortRet;
import com.nus.cool.core.cohort.storage.OLAPRet;
import com.nus.cool.queryserver.singleton.HDFSConnection;
import com.nus.cool.queryserver.singleton.ModelConfig;
import com.nus.cool.queryserver.singleton.ZKConnection;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import org.apache.zookeeper.KeeperException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


/**
 * DistributedController.
 */
@RestController
@RequestMapping("/dist")
public class DistributedController {

  /**
   * CohortAnalysis controller.
   */
  @GetMapping(value = "/cohort")
  public ResponseEntity<String> cohortAnalysis(@Valid @RequestParam Map<String, String> params)
      throws IOException, InterruptedException, URISyntaxException, KeeperException {

    // 1. parser the parameters
    String path = params.get("path");
    String file = params.get("file");
    String queryId = params.get("queryId");

    System.out.println("process file: " + path);

    // 2. connect to zookeeper and HDFS

    HDFSConnection fs = HDFSConnection.getInstance();

    // 3. read query from hdfs
    CohortQueryLayout query = fs.readCohortQuery(queryId);

    String cubeName = query.getDataSource();

    // 4. get Cublet from hdfs
    ByteBuffer buffer = fs.readCublet(path, file);

    // 5. execute the query.
    final long begin = System.currentTimeMillis();

    ModelConfig.cachedCoolModel.reload(cubeName, buffer, fs.readTableSchema(path));

    String inputCohort = query.getInputCohort();
    if (inputCohort != null) {
      System.out.println("Input cohort: " + inputCohort);
      ModelConfig.cachedCoolModel.loadCohorts(inputCohort, cubeName);
    }
    CohortRet resValue =
        performCohortAnalysis(cubeName, "");

    System.out.println("Result for the query is  " + resValue);

    long end = System.currentTimeMillis();
    System.out.println("query elapsed: " + (end - begin));

    // 6. add result to shard storage.
    String content = new ObjectMapper().writeValueAsString(resValue);
    fs.createResult(queryId, content);

    // 7. release one worker.
    ZKConnection zk = ZKConnection.getInstance();
    String workerName = params.get("worker");
    zk.relaseWorker(workerName);

    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body("Done");
  }

  /**
   * IceBergAnalysis controller.
   */
  @GetMapping(value = "/iceberg")
  public ResponseEntity<String> iceBergAnalysis(@RequestParam Map<String, String> params)
      throws IOException, InterruptedException, KeeperException, URISyntaxException {

    // 1. parser the parameters
    String path = params.get("path");
    String file = params.get("file");
    String queryId = params.get("queryId");

    System.out.println("process file: " + path);

    // 2. connect to zookeeper and HDFS
    HDFSConnection fs = HDFSConnection.getInstance();

    // 3. read query from hdfs
    OLAPQueryLayout query = fs.readIcebergQuery(queryId);
    String cubeName = query.getDataSource();

    // 4. get Cublet from hdfs
    ByteBuffer buffer = fs.readCublet(path, file);

    // 5. execute the query.
    long begin = System.currentTimeMillis();

    ModelConfig.cachedCoolModel.reload(cubeName, buffer, fs.readTableSchema(path));

    List<OLAPRet> results =
        performOLAPAnalysis(cubeName, "");
    System.out.println("Result for the query is  " + results);

    long end = System.currentTimeMillis();
    System.out.println("query elapsed: " + (end - begin));

    // 6. add result to shard storage.
    String content = new ObjectMapper().writeValueAsString(results);
    fs.createResult(queryId, content);
    ZKConnection zk = ZKConnection.getInstance();

    // 7. release one worker.
    String workerName = params.get("worker");
    zk.relaseWorker(workerName);

    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body("Done");
  }

}
