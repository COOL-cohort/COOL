package com.nus.cool.core.util.config;

import java.io.File;
import java.io.IOException;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.parser.CsvTupleParser;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.LineTupleReader;
import com.nus.cool.core.util.reader.TupleReader;
import lombok.Getter;

public class CsvDataLoaderConfig extends DataLoaderConfig {

  @Getter
  String[] fieldLine;
  public CsvDataLoaderConfig() {
    super();
  }

  public CsvDataLoaderConfig(long chunkSize, long cubletSize) {
    super(chunkSize, cubletSize);
  }
  
  @Override
  public TupleReader createTupleReader(File dataFile) throws IOException {
    TupleReader reader = new LineTupleReader(dataFile);
    // read the csv column
    if(reader.hasNext()) fieldLine= ((String) reader.next()).split(",");
    return reader;
  }

  @Override
  public TupleParser createTupleParser(TableSchema tableSchema) {
    return new CsvTupleParser();
  }
}
