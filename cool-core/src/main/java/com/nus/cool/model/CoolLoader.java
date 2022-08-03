/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.nus.cool.model;

import com.google.common.io.Files;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.loader.DataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class CoolLoader {

    static final Logger logger = LoggerFactory.getLogger(CoolLoader.class);

    private final DataLoaderConfig loaderConfig;
    /**
     *
     * @param config depends on the data format
     */
    public CoolLoader(DataLoaderConfig config){
        this.loaderConfig = config;
    }

    /**
     *
     * @param dataSourceName output cube name. Need to be specified when loading from the repository
     * @param schemaFileName path to the table.yaml
     * @param dataFileName path to the data.csv
     * @param cubeRepo the name of the output cube repository
     */
    public synchronized void load(String dataSourceName, String schemaFileName, String dataFileName, String cubeRepo) throws IOException{

        // check the existence of the data repository
        File root = new File(cubeRepo);
        if (!root.exists()){
            if (root.mkdir()){
                logger.info("[*] Cube repository " + root.getCanonicalPath() + " is created!");
            } else {
                logger.info("[x] Cube repository " + root.getCanonicalPath() + " already exist!");
            }
        }
        File schemaFile = new File(schemaFileName);
        File dataFile = new File(dataFileName);
        TableSchema schema = TableSchema.read( new FileInputStream(schemaFile));

        // check the existence of the cube
        File cubeRoot = new File(root, dataSourceName);
        if (!cubeRoot.exists()){
            if (cubeRoot.mkdir()){
                logger.info("[*] New cube " + cubeRoot.getCanonicalPath() + " is created!");
            } else {
                logger.info("[x] New cube " + cubeRoot.getCanonicalPath() + "cannot be created!");
            }
        }

        // find all dir, each dir is a version
        File[] allVersions = cubeRoot.listFiles(File::isDirectory);
        int currentVersion = 0;
        if (allVersions != null && allVersions.length!=0){
            Arrays.sort(allVersions);
            File LastVersion = allVersions[allVersions.length - 1];
            currentVersion = Integer.parseInt(LastVersion.getName().substring(1));
        }

        // create a new folder to this new version
        File outputCubeVersionDir = new File(cubeRoot, String.format("v%0"+8+"d",(currentVersion+1)));
        if (outputCubeVersionDir.mkdir()){
            logger.info("[*] New version " + outputCubeVersionDir.getName() + " is created!");
        }
        DataLoader loader = DataLoader.builder(dataSourceName, schema, dataFile, outputCubeVersionDir, this.loaderConfig).build();
        if (!checkConsistency((CsvDataLoaderConfig) this.loaderConfig,schema))
        {
            logger.error("The order of fields in .yaml and .csv is different! Please check it!");
            System.exit(0);
        }
        loader.load();
        // copy the table.yaml to new version folder
        Files.copy(schemaFile, new File(outputCubeVersionDir, "table.yaml"));
    }
    public boolean checkConsistency(CsvDataLoaderConfig conf, TableSchema schema){
        boolean flag=true;
        Map<String, Integer> name2Id = schema.getName2Id();
        for(int i=0;i<name2Id.size();i++){
            if (name2Id.get(conf.getFieldLine()[i])!=i)
                flag=false;
            return flag;
        }
        return flag;
    }

}
