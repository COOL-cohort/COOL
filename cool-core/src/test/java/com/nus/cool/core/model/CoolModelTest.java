
package com.nus.cool.core.model;

import com.nus.cool.functionality.CsvLoaderTest;
import com.nus.cool.model.CoolModel;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

public class CoolModelTest extends CsvLoaderTest  {

    @Test (dataProvider = "CubeListTestDP", dependsOnMethods = "CsvLoaderUnitTest")
    public void CubeListUnitTest(String datasetPath, String[] out) {
        String[] cubes = CoolModel.listCubes(datasetPath);
        assert Arrays.equals(cubes, out);
    }

    @Test(dataProvider = "CubeReloadTestDP", dependsOnMethods = "CsvLoaderUnitTest")
    public static void CubeReloadUnitTest(String datasetPath, String cubeName) throws IOException {
        CoolModel coolModel = new CoolModel(datasetPath);
        coolModel.reload(cubeName);
    }

    @DataProvider(name = "CubeReloadTestDP")
    public Object[][] CubeReloadTestDPArgObjects() {
        String sourcePath = Paths.get(System.getProperty("user.dir"),  "..", "datasetSource").toString();
        return new Object[][] {
                {sourcePath, "health"},
                {sourcePath, "sogamo"},
                {sourcePath, "tpc-h-10g"}
        };
    }

    @DataProvider(name = "CubeListTestDP")
    public Object[][] CubeListArgObjects() {
        String sourcePath = Paths.get(System.getProperty("user.dir"),  "..", "datasetSource").toString();
        return new Object[][] {
                {sourcePath, new String[]{"sogamo", "tpc-h-10g", "health"}},
        };
    }

}
