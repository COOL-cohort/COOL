package com.nus.cool.core.model;

import com.nus.cool.model.CoolModel;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing cool model.
 */
public class CoolModelTest {

  @Test(dataProvider = "CubeListTestDP", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public void cubeListUnitTest(String datasetPath, String[] out) {
    String[] cubes = CoolModel.listCubes(datasetPath);
    for (String cube : out) {
      Assert.assertTrue(Arrays.asList(cubes).contains(cube));
    }
  }

  @Test(dataProvider = "CubeReloadTestDP", dependsOnMethods = {
      "com.nus.cool.functionality.CsvLoaderTest.csvLoaderUnitTest"})
  public static void cubeReloadUnitTest(String datasetPath, String cubeName) throws IOException {
    CoolModel coolModel = new CoolModel(datasetPath);
    coolModel.reload(cubeName);
    coolModel.close();
  }

  /**
   * Data provider for cube reload testing.
   */
  @DataProvider(name = "CubeReloadTestDP")
  public Object[][] cubeReloadTestDPArgObjects() {
    String sourcePath =
        Paths.get(System.getProperty("user.dir"), "..", "CubeRepo/TestCube").toString();
    return new Object[][] {{sourcePath, "health"}, {sourcePath, "sogamo"},
        {sourcePath, "tpc-h-10g"}};
  }

  /**
   * Data provider for cube list operation testing.
   */
  @DataProvider(name = "CubeListTestDP")
  public Object[][] cubeListArgObjects() {
    String sourcePath =
        Paths.get(System.getProperty("user.dir"), "..", "CubeRepo/TestCube").toString();
    return new Object[][] {{sourcePath, new String[] {"sogamo", "tpc-h-10g", "health"}}};
  }

}
