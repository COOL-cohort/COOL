package com.nus.cool.core.io.store;

import java.nio.file.Path;
import java.nio.file.Paths;


import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestTableTest {

    private String sourcePath;
    @BeforeTest
    public void setUp() {
        System.out.println("Start To Test TestTable");
        sourcePath = Paths.get(System.getProperty("user.dir"),
             "src",
             "test",
             "java",
             "com",
             "nus",
             "cool",
             "core",
             "resources").toString();
        System.out.println(sourcePath);
    }

    @Test
    public void BasicPrintTest() {
        String filepath = Paths.get(sourcePath,"TestData","test.csv").toString();
        System.out.println(filepath);
        TestTable table = TestTable.readFromCSV(filepath);
        table.ShowTableHead();
    }
}
