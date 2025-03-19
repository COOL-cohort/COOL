package com.nus.cool.core.util.writer;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.schema.TableSchema;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * CSV data writer.
 */
public class CsvDataWriter implements DataWriter {

  private BufferedWriter writer;
  
  private final String filePath;

  private final TableSchema schema;

  public CsvDataWriter(String filePath, TableSchema schema) {
    this.filePath = filePath;
    this.schema = schema;
  }

  @Override
  public boolean initialize() throws IOException {
    writer = new BufferedWriter(new FileWriter(filePath));
    System.out.println("------CSV data writing initialized------");
    String[] fields = new String[schema.getFields().size()];
    for (int i = 0; i < schema.getFields().size(); i++) {
      fields[i] = schema.getFields().get(i).getName();
    }
    writer.write(String.join(",", fields));
    writer.newLine();
    return true;
  }

  @Override
  public boolean add(FieldValue[] tuple) throws IOException {
    String[] fields = new String[tuple.length];
    for (int i = 0; i < tuple.length; i++) {
      fields[i] = tuple[i].getString();
    }
    writer.write(String.join(",", fields));
    writer.newLine();
    return true;
  }

  @Override
  public void finish() throws IOException {
    writer.flush();
    System.out.println("------CSV data writing finished------");
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }
}