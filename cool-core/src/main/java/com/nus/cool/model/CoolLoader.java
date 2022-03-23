package com.nus.cool.model;

import com.google.common.io.Files;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.loader.DataLoader;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

public class CoolLoader {
    private DataLoaderConfig loaderConfig;
    /**
     *
     * @param config depends on the data format
     */
    public CoolLoader(DataLoaderConfig config){
        this.loaderConfig = config;
    }

    /**
     *
     * @param cube output cube name. Need to be specified when loading from the repository
     * @param schemaFileName path to the table.yaml
     * @param dimFileName path to the dimension.csv
     * @param dataFileName path to the data.csv
     * @param cubeRepo the name of the output cube repository
     * @throws IOException
     */
    public void load(String cube, String schemaFileName, String dimFileName, String dataFileName, String cubeRepo) throws IOException{
        // check the existence of the data repository
        File root = new File(cubeRepo);
        if (!root.exists()){
            if (root.mkdir()){
                System.out.println("[*] Data repository " + root.getCanonicalPath() + " is created!");
            } else {
                System.out.println("[x] Data repository " + root.getCanonicalPath() + "cannot be created!");
            }
        }
        File schemaFile = new File(schemaFileName);
        File dimensionFile = new File(dimFileName);
        File dataFile = new File(dataFileName);
        TableSchema schema = TableSchema.read( new FileInputStream(schemaFile));

        // check the existence of the cube
        File cubeRoot = new File(root, cube);
        if (!cubeRoot.exists()){
            if (cubeRoot.mkdir()){
                System.out.println("[*] New Repo " + cubeRoot.getCanonicalPath() + " is created!");
            } else {
                System.out.println("[x] New Repo " + cubeRoot.getCanonicalPath() + "cannot be created!");
            }
        }

        // version control
        File[] versions = cubeRoot.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
        int currentVersion = 0;
        if (versions.length!=0){
            Arrays.sort(versions);
            File LastVersion = versions[versions.length - 1];
            currentVersion = Integer.parseInt(LastVersion.getName().substring(1));
        }

        File outputCubeVersionDir = new File(cubeRoot, "v"+String.valueOf(currentVersion+1));
        if (outputCubeVersionDir.mkdir()){
            System.out.println("[*] New version " + outputCubeVersionDir.getName() + " is created!");
        }
        DataLoader loader = DataLoader.builder(cube, schema, dimensionFile, dataFile, outputCubeVersionDir, this.loaderConfig).build();
        loader.load();
        Files.copy(schemaFile, new File(outputCubeVersionDir, "table.yaml"));
    }


}
