package com.nus.cool.queryserver.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.CohortProcessor;
import com.nus.cool.core.cohort.CohortQueryLayout;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;
import com.nus.cool.queryserver.singleton.ModelConfig;
import com.nus.cool.queryserver.utils.Util;
import java.io.File;
import java.io.IOException;
import javax.ws.rs.QueryParam;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
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
    System.out.println("[*] Server is listing all cubes.");
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
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public ResponseEntity<String> cohortSelection(
      @RequestParam("queryFile") MultipartFile queryFile) throws IOException {
    System.out.println("[*] Server is performing the cohort query, " + queryFile);
    String queryContent = new String(queryFile.getBytes());
    ObjectMapper mapper = new ObjectMapper();
    CohortQueryLayout layout = mapper.readValue(queryContent, CohortQueryLayout.class);
    String cube = layout.getDataSource();
    ModelConfig.cachedCoolModel.reload(cube);
    System.out.println("reload success...");
    CohortProcessor cohortProcessor = new CohortProcessor(layout);
    CubeRS cubeRS = ModelConfig.cachedCoolModel.getCube(cohortProcessor.getDataSource());
    String res = cohortProcessor.process(cubeRS).toString();
    File currentVersion =
        ModelConfig.cachedCoolModel.getLatestVersion(cohortProcessor.getDataSource());
    cohortProcessor.persistCohort(currentVersion.toString());
    return ResponseEntity.ok().body(res);
  }

  /**
   * performCohortAnalysis.
   */
  @PostMapping(value = "/cohort-analysis",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public ResponseEntity<String> performCohortAnalysis(
      @RequestParam("queryFile") MultipartFile queryFile) throws IOException {
    System.out.println("[*] Server serve cohort analysis, " + queryFile);
    // 1. analysis query
    String queryContent = new String(queryFile.getBytes());
    ObjectMapper mapper = new ObjectMapper();
    CohortQueryLayout layout = mapper.readValue(queryContent, CohortQueryLayout.class);
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
    String res = cohortProcessor.process(cubeRS).toString();
    return ResponseEntity.ok().body(res);
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
