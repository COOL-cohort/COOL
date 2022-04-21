package com.nus.cool.core.io.store;

import java.nio.file.Paths;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestTableTest {
    @BeforeTest
    public void setUp() {
        System.out.println("Start To Test TestTable");
    }

    @Test
    public void BasicPrintTest() {
        String filepath = Paths.get("../TestData/test.csv").toAbsolutePath().toString();
        TestTable table = TestTable.readFromCSV(filepath);
        table.ShowTableHead();
    }
}
