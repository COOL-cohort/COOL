package com.nus.cool.extension.util.config;

import java.io.File;
import java.io.IOException;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.parser.JsonStringTupleParser;
import com.nus.cool.core.util.reader.TupleReader;
import com.nus.cool.extension.util.reader.ParquetTupleReader;

public class ParquetDataLoaderConfig extends DataLoaderConfig{
  public ParquetDataLoaderConfig(long chunkSize, long cubletSize) {
    super(chunkSize, cubletSize);
  }
  
  @Override
  public TupleReader createTupleReader(File dataFile)
    throws IOException {
    return new ParquetTupleReader(dataFile.getAbsolutePath());
  }

  @Override
  public TupleParser createTupleParser(TableSchema tableSchema) {
    return new JsonStringTupleParser(tableSchema);
  }
}
