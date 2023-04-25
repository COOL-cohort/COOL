package com.nus.cool.queryserver.handler;

import static com.nus.cool.functionality.CohortAnalysis.performCohortAnalysis;
import static com.nus.cool.functionality.OLAPAnalysis.performOLAPAnalysis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.cohort.storage.CohortRet;
import com.nus.cool.core.cohort.storage.OLAPRet;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.model.CoolModel;
import com.nus.cool.queryserver.singleton.HDFSConnection;
import com.nus.cool.queryserver.singleton.ModelConfig;
import com.nus.cool.queryserver.singleton.ZKConnection;
import com.nus.cool.result.ExtendedResultTuple;
import javax.validation.Valid;
import org.apache.zookeeper.KeeperException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dist")
public class DistributedController {

  @GetMapping(value = "/cohort")
  public ResponseEntity<String> CohortAnalysis(@Valid @RequestParam Map<String, String> params)
      throws IOException, InterruptedException, URISyntaxException, KeeperException {

    // 1. parser the parameters
    String path = params.get("path");
    String file = params.get("file");
    String queryId = params.get("queryId");
    String workerName = params.get("worker");
    System.out.println("process file: " + path);

    // 2. connect to zookeeper and HDFS
    ZKConnection zk;
    HDFSConnection fs;
    zk = ZKConnection.getInstance();
    fs = HDFSConnection.getInstance();

    // 3. read query from hdfs
    ExtendedCohortQuery query = fs.readCohortQuery(queryId);

    String cubeName = query.getDataSource();

    // 4. get Cublet from hdfs
    ByteBuffer buffer = fs.readCublet(path, file);

    // 5. execute the query.
    QueryResult results;
    long begin = System.currentTimeMillis();

    ModelConfig.cachedCoolModel.reload(cubeName, buffer, fs.readTableSchema(path));

    String inputCohort = query.getInputCohort();
    if (inputCohort != null) {
      System.out.println("Input cohort: " + inputCohort);
      ModelConfig.cachedCoolModel.loadCohorts(inputCohort, cubeName);
    }
    CohortRet resValue =
        performCohortAnalysis(cubeName, "");
    results = QueryResult.ok(resValue);

    System.out.println("Result for the query is  " + results);

    long end = System.currentTimeMillis();
    System.out.println("query elapsed: " + (end - begin));

    // 6. add result to shard storage.
    String content = new ObjectMapper().writeValueAsString(results);
    fs.createResult(queryId, content);

    // 7. release one worker.
    zk.relaseWorker(workerName);

    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body("Done");
  }

  @GetMapping(value = "/iceberg")
  public ResponseEntity<String> IceBergAnalysis(@RequestParam Map<String, String> params)
      throws IOException, InterruptedException, KeeperException, URISyntaxException {

    // 1. parser the parameters
    String path = params.get("path");
    String file = params.get("file");
    String queryId = params.get("queryId");
    String workerName = params.get("worker");

    System.out.println("process file: " + path);

    // 2. connect to zookeeper and HDFS
    ZKConnection zk = ZKConnection.getInstance();
    HDFSConnection fs = HDFSConnection.getInstance();

    // 3. read query from hdfs
    IcebergQuery query = fs.readIcebergQuery(queryId);
    String cubeName = query.getDataSource();

    // 4. get Cublet from hdfs
    ByteBuffer buffer = fs.readCublet(path, file);

    // 5. execute the query.
    long begin = System.currentTimeMillis();

    ModelConfig.cachedCoolModel.reload(cubeName, buffer, fs.readTableSchema(path));

    List<OLAPRet> results =
        performOLAPAnalysis(cubeName, "");
    QueryResult result = QueryResult.ok(results);
    System.out.println("Result for the query is  " + results);

    long end = System.currentTimeMillis();
    System.out.println("query elapsed: " + (end - begin));

    // 6. add result to shard storage.
    String content = new ObjectMapper().writeValueAsString(result);
    fs.createResult(queryId, content);

    // 7. release one worker.
    zk.relaseWorker(workerName);

    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body("Done");
  }

}
