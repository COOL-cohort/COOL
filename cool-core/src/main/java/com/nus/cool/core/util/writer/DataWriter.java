package com.nus.cool.core.util.writer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for data writers.
 */
public interface DataWriter extends Closeable {

  /**
   * Initialize the writer.
   *
   * @return successful initialization
   */
  boolean initialize() throws IOException;

  /**
   * Add a tuple to the data set.
   *
   * @param tuple tuple to add
   * @return status
   */
  boolean add(Object tuple) throws IOException;

  /**
   * Finish writing at the end.
   */
  void finish() throws IOException;
}
