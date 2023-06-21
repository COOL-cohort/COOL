package com.nus.cool.core.cohort;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.common.base.Optional;
import com.nus.cool.core.cohort.storage.CohortRet;
import com.nus.cool.core.cohort.storage.CohortWSStr;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;

/**
 * a collection of methods to persist cohort processing result (CohortRet).
 */
public interface CohortWriter {

  /**
   * setup the directory to be used by other CohortWriter utilities.
   * a recommended path format: cube-repo/cohort/queryName.
   */
  public static boolean setUpOutputFolder(String outputPath) {
    return new File(outputPath).mkdirs();
  }
  
  /**
   * Persist cohort results file to output disk to the same level with the .dz file.
   * E,g. ../CubeRepo/health_raw/v00000012/cohort/queryName/all.cohort.
   *
   * @param outputDir the output file path
   * @throws IOException IOException
   */
  public static void persistCohortResult(CohortRet res, String outputDir) throws IOException {
    try {
      ObjectMapper mapper = new ObjectMapper();
      String cohortJson = Paths.get(outputDir, "query_res.json").toString();
      mapper.writeValue(new File(cohortJson), res.genResult());
    } catch (JsonGenerationException e) {
      System.out.println("failed to generate json for result: " + e);
    } catch (JsonMappingException e) {
      System.out.println("failed to generate json for result: " + e);
    }
  }

  /**
   * Persist the cohort write store into a file.
   */
  public static void persistCohortWS(String cohortName, Optional<CohortWSStr> cohort,
      String outputDir) {
    try {
      if (!cohort.isPresent()) {
        System.out.println("failed to persist cohort: " + cohortName + " " + "(not found)");
        return;
      }
      String fileName = cohortName + ".cohort";
      File cohortResFile = new File(outputDir, fileName);      
      DataOutputStream out = new DataOutputStream(new FileOutputStream(cohortResFile));
      cohort.get().writeTo(out);
      out.close();
    } catch (IOException e) {
      System.out.println("failed to persist cohort: " + cohortName + " " + e);
    }
  }

  /**
   * Output the users of a cohort to file.
   *
   * @param res cohort processing result
   * @param cohortName the cohort to output
   * @param outputDir the directory to store the file
   */
  public static void persistOneCohort(CohortRet res, String cohortName, String outputDir) {
    Optional<CohortWSStr> cohort = res.genCohortUser(cohortName);
    persistCohortWS(cohortName, cohort, outputDir);
  }

  /**
   * Persist a set of cohorts from the result into individual files.
   *
   * @param picked named cohorts to write out.
   */
  public static void persistSelectedCohorts(CohortRet res, Set<String> picked, String outputDir) {
    picked.stream().forEach(x -> persistOneCohort(res, x, outputDir));
  }

  /**
   * Persist all non-empty cohorts from the result into individual files.
   */
  public static void persistAllCohorts(CohortRet res, String outputDir) {
    res.genAllCohortUsers()
        .entrySet()
        .stream()
        .forEach(x -> persistCohortWS(x.getKey(), x.getValue(), outputDir));
  }
}
