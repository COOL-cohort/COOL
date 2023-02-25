package com.nus.cool.extension.util.config;

import com.nus.cool.core.field.ValueConverter;
import com.nus.cool.core.field.ValueConverterConfig;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.core.util.parser.JsonStringTupleParser;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.TupleReader;
import com.nus.cool.extension.util.reader.ParquetTupleReader;
import java.io.File;
import java.io.IOException;

/**
 * Data loader configuration for parquet.
 */
public class ParquetDataLoaderConfig extends DataLoaderConfig {
  
  public ParquetDataLoaderConfig() {
    super();
  }

  public ParquetDataLoaderConfig(long chunkSize, long cubletSize) {
    super(chunkSize, cubletSize);
  }
  
  @Override
  public TupleReader createTupleReader(File dataFile) throws IOException {
    return new ParquetTupleReader(dataFile.getAbsolutePath());
  }

  @Override
  public TupleParser createTupleParser(TableSchema tableSchema, ValueConverterConfig vcConfig) {
    return new JsonStringTupleParser(tableSchema, new ValueConverter(tableSchema, vcConfig));
  }
}
