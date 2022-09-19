package com.nus.cool.core.util.writer;

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
  public boolean add(Object tuple) throws IOException {
    if (!(tuple instanceof String[])) {
      System.out.println(
          "Unexpected tuple type: tuple not in valid type for DataWriter");
      return false;
    }
    String[] fields = (String[]) tuple;
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
