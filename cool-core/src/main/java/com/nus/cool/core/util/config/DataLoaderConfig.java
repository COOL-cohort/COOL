package com.nus.cool.core.util.config;

import java.io.File;
import java.io.IOException;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.TupleReader;

import lombok.AllArgsConstructor;
import lombok.Data;
/**
 * Configuration of data loader.
 */
@Data
@AllArgsConstructor
public abstract class DataLoaderConfig {
  /**
   * the threshold value to start new chunk (default 64 KB)
   */
  public final long chunkSize;
  /**
   * the threshold value to start new cublet (default 1 GB)
   */
  public final long cubletSize; 

  public DataLoaderConfig() {
    this.chunkSize = 65536;
    this.cubletSize = 1 << 30;
  }

  public abstract TupleReader createTupleReader(File dataFile)
    throws IOException;

  public abstract TupleParser createTupleParser(TableSchema tableSchema);
}
