package com.nus.cool.functionality;

import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.model.CoolLoader;

import java.io.IOException;

public class CsvLoader {
    /**
     * Please list the necessary dataset files for the COOL system to load into a new cube.
     * @param args there are five arguments. List in input order
     *  (1) output cube name: to be specified when loading from the repository
     *  (2) table.yaml (3) data.csv (4) output cube repository
     */
    public static void main(String[] args) {
        String cube = args[0];
        String schemaFileName = args[1];
        String dataFileName = args[2];
        String cubeRepo = args[3];

        try {
            DataLoaderConfig config = new CsvDataLoaderConfig();
            CoolLoader coolLoader = new CoolLoader(config);
            coolLoader.load(cube,schemaFileName,dataFileName,cubeRepo);
        } catch (IOException e){
            System.out.println(e);
        }
        System.out.println("Cube " + cube + " is loaded successfully from the CSV format data.");

    }
}
