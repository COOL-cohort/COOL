package com.nus.cool.extension.util.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.TupleReader;
import com.nus.cool.extension.util.arrow.ArrowRowView;
import com.nus.cool.extension.util.reader.ArrowIPCFileTupleReader;

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
  public TupleParser createTupleParser(TableSchema tableSchema) {
    return new TupleParser() {
      @Override
      public String[] parse(Object tuple) {
        if (!(tuple instanceof ArrowRowView)) {
          System.out.println("Unexpected tuple type: not ArrowRowView.");
          return new String[0];
        }

        ArrowRowView arrowTuple = (ArrowRowView) tuple;
        
        return tableSchema.getFields()
                          .stream()
                          .map(x -> arrowTuple.getField(x.getName())
                                              .map(s -> s.toString())
                                              .orElse("null"))
                          .toArray(String[]::new);
      }
    };
  }
}
