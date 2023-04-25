package com.nus.cool.queryserver.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.CohortQueryLayout;
import com.nus.cool.queryserver.model.QueryServerModel;
import com.nus.cool.queryserver.utils.Util;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.ws.rs.*;
import java.io.IOException;


@RestController
@RequestMapping("/cohort")
public class CohortController {

  @GetMapping(value = "/list_cubes",
      produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String[]> listCubes() {
    Util.getTimeClock();
    System.out.println("[*] Server is listing all cubes.");
    return QueryServerModel.listCubes();
  }

  @GetMapping(value = "/list_cube_version",
      produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String[]> listCubeVersions(@QueryParam("cube") String cube)
      throws IOException {
    System.out.println("[*] Server serve listCubeVersions, cubeName = " + cube);
    return QueryServerModel.listCubeVersions(cube);
  }

  @GetMapping(value = "/list_cube_columns",
      produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String[]> listCubeColumns(@QueryParam("cube") String cube)
      throws IOException {
    System.out.println("[*] Server is listing all cohorts." + cube);
    return QueryServerModel.listCubeColumns(cube);
  }

  @GetMapping(value = "/list_cohorts",
      produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String[]> listCohorts(@QueryParam("cube") String cube) throws IOException {
    System.out.println("[*] Server is listing all cohorts." + cube);
    return QueryServerModel.listCohorts(cube);
  }


  @PostMapping(value = "/selection",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public ResponseEntity<String> cohortSelection(
      @RequestParam("queryFile") MultipartFile queryFile) throws IOException {
    System.out.println("[*] Server is performing the cohort query, " + queryFile);
    String queryContent = new String(queryFile.getBytes());
    ObjectMapper mapper = new ObjectMapper();
    CohortQueryLayout q = mapper.readValue(queryContent, CohortQueryLayout.class);
    return QueryServerModel.cohortSelection(q);
  }

  @PostMapping(value = "/cohort-analysis",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public ResponseEntity<String> performCohortAnalysis(
      @RequestParam("queryFile") MultipartFile queryFile) throws IOException {
    System.out.println("[*] Server serve cohort analysis, " + queryFile);
    String queryContent = new String(queryFile.getBytes());
    ObjectMapper mapper = new ObjectMapper();
    CohortQueryLayout q = mapper.readValue(queryContent, CohortQueryLayout.class);
    return QueryServerModel.cohortAnalysis(q);
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
