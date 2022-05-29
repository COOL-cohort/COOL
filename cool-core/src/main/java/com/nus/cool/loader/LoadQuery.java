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

package com.nus.cool.loader;

import lombok.Data;

import java.io.File;
import java.io.IOException;

@Data
public class LoadQuery {
    private String dataFileType;
    private String cubeName;
    private String schemaPath;
    private String dataPath;
    private String outputPath;
    private String configPath;

    // after writing dz, table file, records the path inside the server.
    private String dzFilePath;
    private String TableFilePath;

    public boolean isValid() throws IOException {
        boolean f = true;
        if (dataFileType == "AVRO") f = isExist(configPath);
        return f && isExist(schemaPath) && isExist(dataPath) && cubeName.isEmpty() && outputPath.isEmpty();
    }

    private boolean isExist(String path) throws IOException {
        File f = new File(path);
        if (!f.exists()){
            throw new IOException("[x] File " + path + " does not exist.");
        }
        return true;
    }
}
