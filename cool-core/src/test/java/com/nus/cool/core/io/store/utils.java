package com.nus.cool.core.io.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import com.nus.cool.core.schema.TableSchema;


public class utils {

    public static TestTable loadTable(String dirPath){
        String path = Paths.get(dirPath, "table.csv").toString();
        return TestTable.readFromCSV(path);
    }

    public static TableSchema loadSchema(String dirPath) throws IOException{
        File yamlFile = new File(dirPath, "table.yaml");
        return TableSchema.read(yamlFile);
    }
}
