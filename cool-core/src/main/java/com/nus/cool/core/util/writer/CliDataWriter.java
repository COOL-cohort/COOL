package com.nus.cool.core.util.writer;

import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class CliDataWriter implements DataWriter {

  // It decides to print how much information.
  public boolean printLevel;

  // It stores the output data
  public ArrayList<String> outData;

  public CliDataWriter(ArrayList<String> out, boolean pLevel){
    printLevel = pLevel;
    outData = out;
  }
  
  @Override
  public boolean Initialize() throws IOException {
    if (printLevel) System.out.println("------Display data set------");
    return true;
  }

  @Override
  public boolean Add(Object tuple) throws IOException {
    if (!(tuple instanceof String[])) {
      System.out.println(
          "Unexpected tuple type: tuple not in valid type for DataWriter");
      return false;
    }
    String[] fields = (String[]) tuple;
    outData.add(Arrays.toString(fields));
    if (printLevel) System.out.println(Arrays.toString(fields));
    return false;
  }

  @Override
  public void Finish() throws IOException {
    if (printLevel) System.out.println("------Data set finished------");
  }

  @Override
  public void close() throws IOException {
    // no-op    
  }
}
