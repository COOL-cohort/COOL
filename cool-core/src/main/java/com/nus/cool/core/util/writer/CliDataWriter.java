package com.nus.cool.core.util.writer;

import com.nus.cool.core.field.FieldValue;
import java.io.IOException;
import java.util.Arrays;

/**
 * Command line interface data writer.
 */
public class CliDataWriter implements DataWriter {

  @Override
  public boolean initialize() throws IOException {
    System.out.println("------Display data set------");
    return true;
  }

  @Override
  public boolean add(FieldValue[] tuple) throws IOException {
    String[] fields = new String[tuple.length];
    for (int i = 0; i < tuple.length; i++) {
      fields[i] = tuple[i].getString();
    }
    System.out.println(Arrays.toString(fields));
    return false;
  }

  @Override
  public void finish() throws IOException {
    System.out.println("------Data set finished------");
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
