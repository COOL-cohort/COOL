package com.nus.cool.core.util.writer;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * ListDataWriter write the serialized representation of the records to a list
 *  (It is currently used to generate the respones of cohort exploration
 *  in query server) 
 */
public class ListDataWriter implements DataWriter {

  // It stores the output data
  private List<String> out;

  public ListDataWriter(List<String> out){
    this.out = checkNotNull(out);
  }
  
  @Override
  public boolean Initialize() throws IOException {
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
    out.add(Arrays.toString(fields));
    return false;
  }

  @Override
  public void Finish() throws IOException {
    // no-op
  }

  @Override
  public void close() throws IOException {
    // no-op    
  }
}
