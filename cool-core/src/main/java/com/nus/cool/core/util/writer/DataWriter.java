package com.nus.cool.core.util.writer;

import java.io.Closeable;
import java.io.IOException;

public interface DataWriter extends Closeable {
  
  /**
   * Initialize the writer
   * @Params fileName: file name of dz
   * @return successful initialization
   * @throws IOException
   */
  boolean Initialize(String... fileName) throws IOException;

  /**
   * Add a tuple to the data set
   * 
   * @param tuple
   * @return status
   * @throws IOException
   */
  boolean Add(Object tuple) throws IOException;

  /**
   * finish writing at the end
   * 
   * @throws IOException
   */
  void Finish() throws IOException;
}
