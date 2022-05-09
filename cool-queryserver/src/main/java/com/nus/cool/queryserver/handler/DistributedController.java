package com.nus.cool.queryserver.handler;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.model.CoolModel;
import com.nus.cool.queryserver.singleton.HDFSConnection;
import com.nus.cool.queryserver.singleton.ModelPathCfg;
import com.nus.cool.queryserver.singleton.ZKConnection;
import com.nus.cool.result.ExtendedResultTuple;
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
    public ResponseEntity<String> CohortAnalysis(@RequestParam Map<String, String> params){

        // 1. parser the parameters
        System.out.println("process file: ");
        String path = params.get("path");
        String file = params.get("file");
        String queryId = params.get("queryId");
        String workerName = params.get("worker");

        // 2. connect to zookeeper and HDFS
        ZKConnection zk;
        try {
            zk = ZKConnection.getInstance();
        } catch ( IOException | InterruptedException e) {
            System.out.println("ZKConnection Error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("ZKConnection Error");
        }
        HDFSConnection fs;
        try {
            fs = HDFSConnection.getInstance();
        } catch ( IOException | URISyntaxException e) {
            System.out.println("HDFSConnection Error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("HDFSConnection Error");
        }

        // 3. read query from hdfs
        ExtendedCohortQuery query;
        try {
            query = fs.readCohortQuery(queryId);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("get query Error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("get query Error");
        }
        String cubeName = query.getDataSource();

        // 4. get Cublet from hdfs
        ByteBuffer buffer;
        try {
            buffer = fs.readCublet(path, file);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("get cublet error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("get cublet error");
        }

        // 5. execute the query.
        QueryResult results;
        try {
            long begin = System.currentTimeMillis();

            CoolModel coolModel = new CoolModel(ModelPathCfg.dataSourcePath);
            coolModel.reload(cubeName, buffer, fs.readTableSchema(path));

            String inputCohort = query.getInputCohort();
            if (inputCohort != null) {
                System.out.println("Input cohort: " + inputCohort);
                coolModel.loadCohorts(inputCohort, cubeName);
            }
            InputVector userVector = coolModel.getCohortUsers(inputCohort);
            List<ExtendedResultTuple> resValue =
                    coolModel.cohortEngine.performCohortQuery(coolModel.getCube(cubeName), userVector, query);
            results = QueryResult.ok(resValue);

            System.out.println("Result for the query is  " + results);

            long end = System.currentTimeMillis();
            System.out.println("query elapsed: " + (end - begin));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("execute query error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("execute query error");
        }

        // 6. add result to shard storage.
        try {
            String content = new ObjectMapper().writeValueAsString(results);
            fs.createResult(queryId, content);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("write results error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("write results error");
        }

        // 7. release one worker.
        try {
            zk.relaseWorker(workerName);
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
            System.out.println("release worker error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("release worker error");
        }

        return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body("Done");
    }

    @GetMapping(value = "/iceberg")
    public ResponseEntity<String> IceBergAnalysis(@RequestParam Map<String, String> params){

        System.out.println("version0.0.1");

        // 1. parser the parameters
        System.out.println("process file: ");
        String path = params.get("path");
        String file = params.get("file");
        String queryId = params.get("queryId");
        String workerName = params.get("worker");

        // 2. connect to zookeeper and HDFS
        ZKConnection zk;
        try {
            zk = ZKConnection.getInstance();
        } catch ( IOException | InterruptedException e) {
            System.out.println("ZKConnection Error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("ZKConnection Error");
        }
        HDFSConnection fs;
        try {
            fs = HDFSConnection.getInstance();
        } catch ( IOException | URISyntaxException e) {
            System.out.println("HDFSConnection Error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("HDFSConnection Error");
        }

        // 3. read query from hdfs
        IcebergQuery query;
        try {
            query = fs.readIcebergQuery(queryId);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("get query Error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("get query Error");
        }
        String cubeName = query.getDataSource();

        // 4. get Cublet from hdfs
        ByteBuffer buffer;
        try {
            buffer = fs.readCublet(path, file);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("get cublet error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("get cublet error");
        }

        // 5. execute the query.
        QueryResult result;
//        List<BaseResult> results;
        try {
            long begin = System.currentTimeMillis();

            CoolModel coolModel = new CoolModel(ModelPathCfg.dataSourcePath);
            coolModel.reload(cubeName, buffer, fs.readTableSchema(path));

            List<BaseResult> results = coolModel.olapEngine.performOlapQuery(coolModel.getCube(cubeName), query);
            result = QueryResult.ok(results);
            System.out.println("Result for the query is  " + results);

            long end = System.currentTimeMillis();
            System.out.println("query elapsed: " + (end - begin));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("execute query error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("execute query error");
        }

        // 6. add result to shard storage.
        try {
            String content = new ObjectMapper().writeValueAsString(result);
            fs.createResult(queryId, content);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("write results error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("write results error");
        }

        // 7. release one worker.
        try {
            zk.relaseWorker(workerName);
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
            System.out.println("release worker error");
            return ResponseEntity.badRequest().headers(HttpHeaders.EMPTY).body("release worker error");
        }

        return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body("Done");
    }

}
