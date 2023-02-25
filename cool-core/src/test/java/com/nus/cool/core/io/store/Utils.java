package com.nus.cool.core.io.store;

import java.nio.file.Paths;

/**
 * Utilities to load data and schema.
 */
public class Utils {

  /**
   * Load table from the specified directory.
   */
  public static TestTable loadTable(String dirPath) {
    String path = Paths.get(dirPath, "table.csv").toString();
    String schemaPath = Paths.get(dirPath, "table.yaml").toString();
    return TestTable.readFromCSV(path, schemaPath);
  }
}
