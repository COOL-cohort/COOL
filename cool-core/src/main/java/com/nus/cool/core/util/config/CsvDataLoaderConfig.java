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

  public CsvDataLoaderConfig() {
    super();
  }

  public CsvDataLoaderConfig(long chunkSize, long cubletSize) {
    super(chunkSize, cubletSize);
  }

  public String[] data_fieldName;

  @Override
  public TupleReader createTupleReader(File dataFile) throws IOException {
    TupleReader reader = new LineTupleReader(dataFile);
    // read the csv column
    if(reader.hasNext()) {
      String[] line =  ((String) reader.next()).split(",");
      this.data_fieldName=line;
    }
    return reader;
  }

  @Override
  public TupleParser createTupleParser(TableSchema tableSchema) {
    return new CsvTupleParser();
  }
}
