package com.nus.cool.queryserver.model;

import com.nus.cool.core.cohort.CohortProcessor;
import com.nus.cool.core.cohort.CohortQueryLayout;
import com.nus.cool.core.cohort.OLAPProcessor;
import com.nus.cool.core.cohort.OLAPQueryLayout;
import com.nus.cool.core.cohort.storage.OLAPRet;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.extension.util.config.AvroDataLoaderConfig;
import com.nus.cool.extension.util.config.ParquetDataLoaderConfig;
import com.nus.cool.loader.LoadQuery;
import com.nus.cool.model.CoolLoader;
import com.nus.cool.model.CoolModel;
import com.nus.cool.queryserver.singleton.ModelConfig;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;


/**
 * QueryServerModel.
 */
public class QueryServerModel {

  /**
   * List all existing cubes.
   *
   * @return cubes
   */
  public static ResponseEntity<String[]> listCubes() {
    return ResponseEntity.ok().headers(HttpHeaders.EMPTY)
        .body(CoolModel.listCubes(ModelConfig.dataSourcePath));
  }

  /**
   * List all version of cube.
   *
   * @return cubes
   */
  public static ResponseEntity<String[]> listCubeVersions(String cube) throws IOException {
    ModelConfig.cachedCoolModel.reload(cube);
    String[] versions = ModelConfig.cachedCoolModel.getAllVersions(cube);
    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body(versions);
  }

  /**
   * List all columns of cube.
   *
   * @return cubes
   */
  public static ResponseEntity<String[]> listCubeColumns(String cube) throws IOException {
    ModelConfig.cachedCoolModel.reload(cube);
    String[] cols = ModelConfig.cachedCoolModel.getCubeColumns(cube);
    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body(cols);
  }

  /**
   * List all existing cohorts.
   *
   * @return cubes
   */
  public static ResponseEntity<String[]> listCohorts(String cube) throws IOException {
    ModelConfig.cachedCoolModel.reload(cube);
    String[] cohorts = ModelConfig.cachedCoolModel.listCohorts(cube);
    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body(cohorts);
  }

  /**
   * cohort selection.
   *
   * @return cubes
   */
  public static ResponseEntity<String> cohortSelection(CohortQueryLayout layout)
      throws IOException {
    String cube = layout.getDataSource();
    ModelConfig.cachedCoolModel.reload(cube);
    System.out.println("reload success...");
    CohortProcessor cohortProcessor = new CohortProcessor(layout);
    CubeRS cubeRS = ModelConfig.cachedCoolModel.getCube(cohortProcessor.getDataSource());
    String res = cohortProcessor.process(cubeRS).toString();

    File currentVersion =
        ModelConfig.cachedCoolModel.getLatestVersion(cohortProcessor.getDataSource());
    cohortProcessor.persistCohort(currentVersion.toString());
    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body(res);
  }

  /**
   * cohort analysis.
   *
   * @return result
   */
  public static ResponseEntity<String> cohortAnalysis(CohortQueryLayout layout) throws IOException {
    // get cubeRS
    String cube = layout.getDataSource();
    ModelConfig.cachedCoolModel.reload(cube);
    CohortProcessor cohortProcessor = new CohortProcessor(layout);
    CubeRS cubeRS = ModelConfig.cachedCoolModel.getCube(cohortProcessor.getDataSource());
    File currentVersion =
        ModelConfig.cachedCoolModel.getCubeStorePath(cohortProcessor.getDataSource());

    // load input cohort
    if (cohortProcessor.getInputCohort() != null) {
      File cohortFile = new File(currentVersion, "cohort/" + cohortProcessor.getInputCohort());
      if (cohortFile.exists()) {
        cohortProcessor.readOneCohort(cohortFile);
      }
    }

    // process
    String res = cohortProcessor.process(cubeRS).toString();
    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body(res);
  }

  /**
   * Perform iceBergQuery.
   *
   * @param layout query
   * @return result
   */
  public static ResponseEntity<String> precessIcebergQuery(OLAPQueryLayout layout)
      throws IOException {
    String inputSource = layout.getDataSource();
    ModelConfig.cachedCoolModel.reload(inputSource);
    OLAPProcessor olapProcessor = new OLAPProcessor(layout);
    // start a new cool model and reload the cube
    CubeRS cube = ModelConfig.cachedCoolModel.getCube(layout.getDataSource());
    List<OLAPRet> ret = olapProcessor.processCube(cube);
    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body(ret.toString());
  }

  /**
   * Load a new cube.
   *
   * @param q query instance
   * @return Response
   */
  public static ResponseEntity<String> loadCube(LoadQuery q) throws IOException {
    q.isValid();
    String fileType = q.getDataFileType().toUpperCase();
    DataLoaderConfig config;
    switch (fileType) {
      case "CSV":
        config = new CsvDataLoaderConfig();
        break;
      case "PARQUET":
        config = new ParquetDataLoaderConfig();
        break;
      case "AVRO":
        config = new AvroDataLoaderConfig(new File(q.getConfigPath()));
        break;
      default:
        throw new IllegalArgumentException("[x] Invalid load file type: " + fileType);
    }
    System.out.println(config.getClass().getName());
    CoolLoader coolLoader = new CoolLoader(config);
    coolLoader.load(q.getCubeName(), q.getSchemaPath(), q.getDataPath(), q.getOutputPath());
    String resStr = "Cube " + q.getCubeName() + " is loaded successfully";
    return ResponseEntity.ok().headers(HttpHeaders.EMPTY).body(resStr);
  }

}
