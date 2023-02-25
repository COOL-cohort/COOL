package com.nus.cool.core.io.store;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueConverter;
import com.nus.cool.core.field.ValueConverterConfig;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.parser.CsvTupleParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import lombok.Getter;

/**
 * Generate TestTable from csv File simply and Generate Data for UnitTest.
 */
public class TestTable {
  @Getter
  private HashMap<String, Integer> field2Ids;
  private ArrayList<String> fields;

  @Getter
  private ArrayList<ArrayList<FieldValue>> cols;

  @Getter
  private int rowCounts;

  @Getter
  private int colCounts;

  @Getter
  private TableSchema schema;

  private TestTable() {
    this.field2Ids = new HashMap<String, Integer>();
    this.fields = new ArrayList<>();
    this.cols = new ArrayList<ArrayList<FieldValue>>();
    this.rowCounts = 0;
    this.colCounts = 0;
  }

  /**
   * return new TestTable object which structured file data.
   */
  public static TestTable readFromCSV(String filepath, String schemaPath) {
    TestTable table = new TestTable();

    try {
      File file = new File(filepath);
      FileReader fr = new FileReader(file);
      BufferedReader br = new BufferedReader(fr);
      
      table.schema = TableSchema.read(new File(schemaPath));
      
      CsvTupleParser csvParser = new CsvTupleParser(
          new ValueConverter(table.schema, new ValueConverterConfig()));
        
      // process header
      String line = br.readLine();
      if (line == null) {
        fr.close();
        return table; // empty
      }
      String[] fieldNames = line.split(",", -1);
      for (int i = 0; i < fieldNames.length; i++) {
        table.field2Ids.put(fieldNames[i], i);
        table.fields.add(fieldNames[i]);
        table.cols.add(new ArrayList<FieldValue>());
      }
      table.colCounts = table.field2Ids.size();
      // parse data
      while ((line = br.readLine()) != null) {
        FieldValue[] vs = csvParser.parse(line);

        for (int i = 0; i < table.field2Ids.size(); i++) {
          table.cols.get(i).add(vs[i]);
        }
        table.rowCounts += 1;
      }
      fr.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return table;
  }

  /**
   * return the tuple at index idx.
   */
  public FieldValue[] getTuple(int idx) {
    FieldValue[] ret = new FieldValue[this.colCounts];
    for (int i = 0; i < this.colCounts; i++) {
      ret[i] = this.cols.get(i).get(idx);
    }
    return ret;
  }

  /**
   * Print all dataItem.
   */
  public void showTable() {
    this.tablePrint(this.rowCounts);
  }

  /**
   * Print first 10 line dataitem.
   */
  public void showTableHead() {
    this.tablePrint(10);
  }

  private void tablePrint(int rows) {
    int rowMax = rows > this.rowCounts ? this.rowCounts : rows;
    // print header

    System.out.println("Table:");
    String line = "|";
    for (String colheader : this.fields) {
      String s = String.format("%15s_%d", colheader, this.field2Ids.get(colheader));
      line += s;
    }
    line += "|";

    int n = line.length() - 2;
    printline(n);
    System.out.println(line);
    printline(n);

    // print value
    for (int row = 0; row < rowMax; row++) {
      line = "| ";
      for (int col = 0; col < this.colCounts; col++) {
        String s = String.format("%15s |", this.cols.get(col).get(row));
        line = String.join("", line, s);
      }
      System.out.println(line);
    }
    printline(n);
  }

  private void printline(int n) {
    String line = "|";
    String s = String.join("", Collections.nCopies(n, "-"));
    line = line + s + line;
    System.out.println(line);
  }

  @Override
  public String toString() {
    return "TestTable [field2Ids=" + field2Ids + ", rowCounts=" + rowCounts + ", colCounts="
        + colCounts + "]";
  }

}
