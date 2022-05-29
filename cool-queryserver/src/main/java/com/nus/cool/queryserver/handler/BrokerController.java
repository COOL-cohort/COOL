package com.nus.cool.queryserver.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.loader.LoadQuery;
import com.nus.cool.queryserver.model.QueryInfo;
import com.nus.cool.queryserver.model.QueryServerModel;
import com.nus.cool.queryserver.singleton.HDFSConnection;
import com.nus.cool.queryserver.singleton.QueryIndex;
import com.nus.cool.queryserver.singleton.TaskQueue;
import com.nus.cool.queryserver.utils.Util;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.nus.cool.queryserver.model.Parameter;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/broker")
public class BrokerController {

    /**
     * Assume the csv file already at the server side. this
     * Load the local CSV file and upload it to hdfs
     * eg. input: '{"dataFileType": "CSV", "cubeName": "sogamo", "schemaPath": "sogamo/table.yaml",
     *      "dimPath": "sogamo/dim.csv", "dataPath": "sogamo/test.csv", "outputPath": "datasetSource"}'
     * @param req request parsed from json.
     * @return response
     * @throws URISyntaxException exception
     * @throws IOException exception
     */
    @PostMapping(value = "/load-data-to-hdfs",
            produces = MediaType.APPLICATION_JSON_VALUE,
            consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> loadDataToDfs(@RequestBody LoadQuery req) throws URISyntaxException, IOException {

        Util.getTimeClock();

        // file name of the .dz
        String fileName = Long.toHexString(System.currentTimeMillis()) + ".dz";

        QueryServerModel.loadCube(req, fileName);

        // 1. connect to hdfs, get data Source Name, cohort or iceberg
        HDFSConnection fs = HDFSConnection.getInstance();

        String localPath1 = req.getOutputPath() + "/" + req.getDzFilePath();;
        String dfsPath1 = "/cube/" + req.getDzFilePath();
        fs.uploadToDfs(localPath1, dfsPath1);

        String localPath2 = req.getOutputPath() + "/" + req.getTableFilePath();
        String dfsPath2 = "/cube/" + req.getTableFilePath();
        fs.uploadToDfs(localPath2, dfsPath2);

        System.out.println("[*] Data and file loaded");
        return  null;
    }

    @GetMapping(value = "/execute")
    public ResponseEntity<String> handler(@RequestParam Map<String, String> params){
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

