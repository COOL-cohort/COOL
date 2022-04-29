package com.nus.cool.functionality;

import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.model.CoolLoader;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;


public class CsvLoaderTest {

    @Test(priority = 10, enabled = true)
    public static void CsvFileExistTest() throws IOException {
        System.out.println("======================== Csv data loader failure Test ========================");
        // System.out.println(System.getProperty("user.dir"));
        CsvProperties csvps= new CsvProperties();
        Properties csvp=csvps.getProperties();
        String cube = csvp.getProperty("cube_0");
        String schemaFileName =  csvp.getProperty("schemaFileName_0");
        String dimFileName =  csvp.getProperty("dimFileName_0");
        String dataFileName = csvp.getProperty("dataFileName_0");
        String cubeRepo =  csvp.getProperty("cubeRepo_0");
        DataLoaderConfig config = new CsvDataLoaderConfig();
        CoolLoader loader = new CoolLoader(config);
        loader.load(cube, schemaFileName, dimFileName, dataFileName, cubeRepo);
        System.out.println("Csv file load correctly");
    }
    @Test(expectedExceptions = {IOException.class}, priority = 10, enabled = true)
    public static void CsvFileNonExistTest() throws IOException {
        System.out.println("======================== Csv data loader failure Test ========================");
        // System.out.println(System.getProperty("user.dir"));
        CsvProperties csvps= new CsvProperties();
        Properties csvp=csvps.getProperties();
        String cube = csvp.getProperty("cube_1");
        String schemaFileName =  csvp.getProperty("schemaFileName_1");
        String dimFileName =  csvp.getProperty("dimFileName_1");
        String dataFileName = csvp.getProperty("dataFileName_1");
        String cubeRepo =  csvp.getProperty("cubeRepo_1");
        DataLoaderConfig config = new CsvDataLoaderConfig();
        CoolLoader loader = new CoolLoader(config);
        loader.load(cube, schemaFileName, dimFileName, dataFileName, cubeRepo);
    }
}