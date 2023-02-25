package com.nus.cool.core.util.config;

import com.nus.cool.core.field.ValueConverter;
import com.nus.cool.core.field.ValueConverterConfig;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.parser.CsvTupleParser;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.LineTupleReader;
import com.nus.cool.core.util.reader.TupleReader;
import java.io.File;
import java.io.IOException;
import lombok.Getter;

/**
 * Configuration for csv file loader.
 */
public class CsvDataLoaderConfig extends DataLoaderConfig {

  public CsvDataLoaderConfig() {
    super();
  }

  public CsvDataLoaderConfig(long chunkSize, long cubletSize) {
    super(chunkSize, cubletSize);
  }

  @Getter
  private String[] dataFieldName;

  @Override
  public TupleReader createTupleReader(File dataFile) throws IOException {
    TupleReader reader = new LineTupleReader(dataFile);
    // read the csv column
    if (reader.hasNext()) {
      String[] line = ((String) reader.next()).split(",");
      this.dataFieldName = line;
    }
    return reader;
  }

  @Override
  public TupleParser createTupleParser(TableSchema tableSchema, ValueConverterConfig vsConfig) {
    return new CsvTupleParser(new ValueConverter(tableSchema, vsConfig));
  }
}
