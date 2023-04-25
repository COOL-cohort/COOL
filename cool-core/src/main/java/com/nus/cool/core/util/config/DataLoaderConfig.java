package com.nus.cool.core.util.config;

import com.nus.cool.core.field.ValueConverterConfig;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.TupleReader;
import java.io.File;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Configuration of data loader.
 */
@Data
@AllArgsConstructor
public abstract class DataLoaderConfig {
  /**
   * The threshold value to start new chunk. (default 64 KB)
   */
  public final long chunkSize;
  /**
   * The threshold value to start new cublet. (default 1 GB)
   */
  public final long cubletSize;

  /**
   * Basic data loader configuration.
   */
  public DataLoaderConfig() {
    // unit: Byte
    this.chunkSize = 65536;
    this.cubletSize = 1 << 30;
  }

  public abstract TupleReader createTupleReader(File dataFile)
      throws IOException;

  public abstract TupleParser createTupleParser(TableSchema tableSchema,
      ValueConverterConfig vcConfig);
}
