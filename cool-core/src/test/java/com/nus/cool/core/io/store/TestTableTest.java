package com.nus.cool.core.io.store;

import java.nio.file.Paths;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Test the stub TestTable impl.
 */
public class TestTableTest {

  private String sourcePath;

  /**
   * setup.
   */
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
  }

  @Test
  public void basicPrintTest() {
    String filepath = Paths.get(sourcePath, "fieldtest", "table.csv").toString();
    TestTable table = TestTable.readFromCSV(filepath);
    table.showTableHead();
  }
}
