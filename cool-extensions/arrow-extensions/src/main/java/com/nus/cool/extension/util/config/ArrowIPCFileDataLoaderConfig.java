package com.nus.cool.extension.util.config;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueConverter;
import com.nus.cool.core.field.ValueConverterConfig;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.TupleReader;
import com.nus.cool.extension.util.arrow.ArrowRowView;
import com.nus.cool.extension.util.reader.ArrowIPCFileTupleReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.arrow.memory.RootAllocator;

/**
 * The DataLoaderConfig to generate TupleReader and TupleParser
 *  to handle files in Apache Arrow IPC file format.
 *  Current support of Arrow is limited considering the FieldType support in
 *  COOL. We assumed that COOL Metric type field matches with arrow vector
 *  in one of the integer type in Arrow. And the rest of the field types take
 *  the string representation of other fields in Arrow.  
 */
public class ArrowIPCFileDataLoaderConfig extends DataLoaderConfig {
  
  public ArrowIPCFileDataLoaderConfig() {
    super();
  }

  public ArrowIPCFileDataLoaderConfig(long chunkSize, long cubletSize) {
    super(chunkSize, cubletSize);
  }

  @Override
  public TupleReader createTupleReader(File dataFile)
      throws IOException {
    FileInputStream fileInputStream = new FileInputStream(dataFile); 
    return new ArrowIPCFileTupleReader(fileInputStream, new RootAllocator());
  }

  @Override
  public TupleParser createTupleParser(TableSchema tableSchema, ValueConverterConfig vcConfig) {
    return new TupleParser() {
      private final ValueConverter converter = new ValueConverter(tableSchema, vcConfig);

      @Override
      public FieldValue[] parse(Object tuple) throws IOException {
        if (!(tuple instanceof ArrowRowView)) {
          throw new IOException("Invalid input to arrow tuple parser");
        }

        ArrowRowView arrowTuple = (ArrowRowView) tuple;
        
        return converter.convert(tableSchema.getFields()
                          .stream()
                          .map(x -> arrowTuple.getField(x.getName())
                                              .map(s -> s.toString())
                                              .orElse("null"))
                          .toArray(String[]::new));
      }
    };
  }
}
