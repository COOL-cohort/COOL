package com.nus.cool.functionality;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CsvProperties {


    public CsvProperties(){

    }

    public Properties getProperties() throws IOException {


        String sourcePath = Paths.get(System.getProperty("user.dir"),
                "src",
                "test",
                "java",
                "com",
                "nus",
                "cool",
                "functionality",
                "resources").toString();
        System.out.println(sourcePath);
        System.out.println("here.."+Paths.get(System.getProperty("user.dir")));

        String filepath = Paths.get(sourcePath,"TestData","csv.properties").toString();

        InputStream input = new FileInputStream(filepath);

        Properties prop = new Properties();

        // load a properties file
        prop.load(input);

        // get the property value and print it out
        System.out.println(prop.getProperty("cube"));
        System.out.println(prop.getProperty("schemaFileName"));
        System.out.println(prop.getProperty("dimFileName"));
        System.out.println(prop.getProperty("dataFileName"));
        System.out.println(prop.getProperty("cubeRepo"));
        return prop;
    }
}