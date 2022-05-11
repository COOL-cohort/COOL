package com.nus.cool.core.util.parser;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;

import java.util.Arrays;

public class CsvTupleParserTest {

    private String sourcePath;
    @BeforeTest
    public void setUp() {
        System.out.println("Start To Test CvsTupleParser");
        sourcePath = Paths.get(System.getProperty("user.dir"),
                "src",
                "test",
                "java",
                "com",
                "nus",
                "cool",
                "core",
                "resources").toString();
    }

    @Test
    public void parseTest(){
        String filepath = Paths.get(sourcePath, "parsertest", "table.csv").toString();
        System.out.println(filepath);
        CsvTupleParser csvParser = new CsvTupleParser();
        try {
            File file = new File(filepath);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            Boolean header = true;
            while ((line = br.readLine()) != null) {
                String[] vs = csvParser.parse(line);
                String expected =Arrays.toString(vs).trim().replaceAll("\\s+","").replace("[","").replace("]","");
                //System.out.println("expected="+expected);
                Assert.assertEquals(line, expected);
            }
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}