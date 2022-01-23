package com.nus.cool.core.util.config;

import java.io.File;
import java.io.IOException;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.TupleReader;

import lombok.Data;
/**
 * Configuration of data loader.
 */
@Data
public abstract class DataLoaderConfig {
  /**
   * the threshold value to start new chunk
   */
  public final long chunkSize;
  /**
   * the threshold value to start new cublet
   */
  public final long cubletSize; 

  public abstract TupleReader createTupleReader(File dataFile)
    throws IOException;

  public abstract TupleParser createTupleParser(TableSchema tableSchema);
}
