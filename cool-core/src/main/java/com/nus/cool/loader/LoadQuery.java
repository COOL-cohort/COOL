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
