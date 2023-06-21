package com.nus.cool.queryserver.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.nus.cool.core.cohort.CohortProcessor;
import com.nus.cool.core.cohort.CohortQueryLayout;
import com.nus.cool.core.cohort.CohortWriter;
import com.nus.cool.core.cohort.cohortselect.CohortSelectionLayout;
import com.nus.cool.core.cohort.storage.CohortRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import com.nus.cool.queryserver.singleton.ModelConfig;
import com.nus.cool.queryserver.utils.Util;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.QueryParam;
import org.apache.avro.data.Json;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


/**
 * CohortController.
 */
@RestController
@RequestMapping("/cohort")
public class CohortController {

  /**
   * list_cubes.
   */
  @GetMapping(value = "/list_cubes",
      produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String[]> listCubes() {
    Util.getTimeClock();
    System.out.println("[*] Server is listing all cubes in: " + ModelConfig.dataSourcePath);
    return ResponseEntity.ok().body(CoolModel.listCubes(ModelConfig.dataSourcePath));
  }

  /**
   * list_cube_version.
   */
  @GetMapping(value = "/list_cube_version",
      produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String[]> listCubeVersions(@QueryParam("cube") String cube)
      throws IOException {
    System.out.println("[*] Server serve listCubeVersions, cubeName = " + cube);
    ModelConfig.cachedCoolModel.reload(cube);
    String[] versions = ModelConfig.cachedCoolModel.getAllVersions(cube);
    return ResponseEntity.ok().body(versions);
  }

  /**
   * list_cube_columns.
   */
  @GetMapping(value = "/list_cube_columns",
      produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String[]> listCubeColumns(@QueryParam("cube") String cube)
      throws IOException {
    System.out.println("[*] Server is listing all cohorts." + cube);
    ModelConfig.cachedCoolModel.reload(cube);
    String[] cols = ModelConfig.cachedCoolModel.getCubeColumns(cube);
    return ResponseEntity.ok().body(cols);
  }

  /**
   * list_cube_columns.
   */
  @GetMapping(value = "/list_col_info",
      produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> listColInfo(@QueryParam("cube") String cube,
                                              @QueryParam("col") String col)
      throws IOException {
    System.out.println("[*] Server is listing the information of the col: " + col + " in " + cube);
    ModelConfig.cachedCoolModel.reload(cube);
    String colInfo = ModelConfig.cachedCoolModel.getCubeMeta(cube).getFieldMeta(col);
    return ResponseEntity.ok().body(colInfo);
  }

  /**
   * list_cohorts.
   */
  @GetMapping(value = "/list_cohorts",
      produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String[]> listCohorts(@QueryParam("cube") String cube) throws IOException {
    System.out.println("[*] Server is listing all cohorts." + cube);
    ModelConfig.cachedCoolModel.reload(cube);
    String[] cohorts = ModelConfig.cachedCoolModel.listCohorts(cube);
    return ResponseEntity.ok().body(cohorts);
  }


  /**
   * selection.
   */
  @PostMapping(value = "/selection",
      produces = MediaType.APPLICATION_JSON_VALUE
  )
  public ResponseEntity<String> cohortSelection(
      // @RequestParam("queryFile") MultipartFile queryFile) throws IOException {
      @RequestBody String queryContent) throws IOException {
    System.out.println("[*] Server is performing the cohort selection query, " + queryContent);
    ObjectMapper mapper = new ObjectMapper();
    CohortQueryLayout layout = mapper.readValue(queryContent, CohortQueryLayout.class);
    layout.initCohortQuery();
    CohortProcessor cohortProcessor = new CohortProcessor(layout);
    String cube = layout.getDataSource();
    ModelConfig.cachedCoolModel.reload(cube);
    CubeRS cubeRS = ModelConfig.cachedCoolModel.getCube(cohortProcessor.getDataSource());
    CohortRet ret = cohortProcessor.process(cubeRS);
    System.out.println(ret.toString());

    // store information
    File currentVersion =
        ModelConfig.cachedCoolModel.getLatestVersion(cohortProcessor.getDataSource());
    // store the cohort
    String outputPath = currentVersion.toString() + "/cohort/" + layout.getQueryName();
    CohortWriter.setUpOutputFolder(outputPath);
    // store the query
    BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath + "/query.json"));
    writer.write(queryContent);
    writer.close();
    // store results query_res.json
    CohortWriter.persistCohortResult(ret, outputPath);

    if (layout.isSaveCohort()) {
      if (layout.selectAll()) {
        String cohortName = layout.getOutputCohort();
        CohortWriter.persistOneCohort(ret, cohortName, outputPath);
      } else {
        CohortWriter.persistAllCohorts(ret, outputPath);
      }
    }

    // read query results
    String cohortStoragePath = Paths.get(outputPath, "query_res.json").toString();
    File jsonFile = new File(cohortStoragePath);
    Reader reader = new InputStreamReader(new FileInputStream(jsonFile), "utf-8");
    int ch = 0;
    StringBuffer sb = new StringBuffer();
    while ((ch = reader.read()) != -1) {
      sb.append((char) ch);
    }
    reader.close();
    return ResponseEntity.ok().body(sb.toString());
  }

  /**
   * performCohortAnalysis.
   */
  @PostMapping(value = "/cohort-analysis",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> performCohortAnalysis(
      // @RequestParam("queryFile") MultipartFile queryFile) throws IOException {
      @RequestBody String queryContent) throws IOException {
    System.out.println("[*] Server is performing the cohort analysis query, " + queryContent);
    // 1. analysis query
    ObjectMapper mapper = new ObjectMapper();
    CohortQueryLayout layout = mapper.readValue(queryContent, CohortQueryLayout.class);
    layout.initCohortQuery();
    String cube = layout.getDataSource();
    // 2. process query
    ModelConfig.cachedCoolModel.reload(cube);
    CohortProcessor cohortProcessor = new CohortProcessor(layout);
    CubeRS cubeRS = ModelConfig.cachedCoolModel.getCube(cohortProcessor.getDataSource());
    File currentVersion =
        ModelConfig.cachedCoolModel.getCubeStorePath(cohortProcessor.getDataSource());

    // 3. load input cohort
    if (cohortProcessor.getInputCohort() != null) {
      File cohortFile = new File(currentVersion, "cohort/" + cohortProcessor.getInputCohort());
      if (cohortFile.exists()) {
        cohortProcessor.readOneCohort(cohortFile);
      }
    }

    // process
    CohortRet ret = cohortProcessor.process(cubeRS);
    System.out.println(ret.toString());

    // store the cohort
    String outputPath = currentVersion.toString() + "/cohort/" + layout.getQueryName();
    CohortWriter.setUpOutputFolder(outputPath);
    // store the query
    BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath + "/query.json"));
    writer.write(queryContent);
    writer.close();
    // store results query_res.json
    CohortWriter.persistCohortResult(ret, outputPath);

    if (layout.isSaveCohort()) {
      if (layout.selectAll()) {
        String cohortName = layout.getOutputCohort();
        CohortWriter.persistOneCohort(ret, cohortName, outputPath);
      } else {
        CohortWriter.persistAllCohorts(ret, outputPath);
      }
    }

    // read query results
    String cohortStoragePath = Paths.get(outputPath, "query_res.json").toString();
    File jsonFile = new File(cohortStoragePath);
    Reader reader = new InputStreamReader(new FileInputStream(jsonFile), "utf-8");
    int ch = 0;
    StringBuffer sb = new StringBuffer();
    while ((ch = reader.read()) != -1) {
      sb.append((char) ch);
    }
    reader.close();
    // Map<String, String> out = new HashMap<String, String>();
    // out.put("results", ret.toString());
    // out.put("cohorts", sb.toString());
    return ResponseEntity.ok().body(ret.toJson().toString());
    // return ResponseEntity.ok().body(res);
  }

  //    @PostMapping(value = "/funnel-analysis",
  //        produces = MediaType.APPLICATION_JSON_VALUE,
  //        consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  //    public ResponseEntity<String> performFunnelAnalysis(
  //        @RequestParam("queryFile") MultipartFile queryFile) throws IOException {
  //      System.out.println("[*] Server is performing the cohort query form IP: ");
  //      System.out.println("[*] This query is for funnel analysis: " + queryFile);
  //      String queryContent = new String(queryFile.getBytes());
  //      ObjectMapper mapper = new ObjectMapper();
  //      FunnelQuery q = mapper.readValue(queryContent, FunnelQuery.class);
  //      return QueryServerModel.funnelAnalysis(q);
  //    }

}
