package com.nus.cool.extension.util.config;

import java.io.File;
import java.io.IOException;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.TupleReader;
import com.nus.cool.extension.util.reader.AvroTupleReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroDataLoaderConfig extends DataLoaderConfig {

  private Schema schema;

  AvroDataLoaderConfig() {}

  public AvroDataLoaderConfig(File avroSchema) throws IOException {
    this.schema = new Schema.Parser().parse(avroSchema);
  }

  public AvroDataLoaderConfig(long chunkSize, long cubletSize, File avroSchema) throws IOException {
    this.schema = new Schema.Parser().parse(avroSchema);
  }

  @Override
  public TupleReader createTupleReader(File dataFile)
    throws IOException {
    return new AvroTupleReader(dataFile, this.schema);
  }

  @Override
  public TupleParser createTupleParser(TableSchema tableSchema) {
    return new TupleParser() {
      @Override
      public String[] parse(Object tuple) {
        if (!(tuple instanceof GenericRecord)) {
          System.out.println(
            "Unexpected tuple type: not avro record");
          return new String[0]; // return an empty string
        }

        GenericRecord avroTuple = (GenericRecord) tuple;

        return tableSchema.getFields().stream().map(x -> avroTuple.get(x.getName()).toString()).toArray(String[]::new);
      }
    };
  }
}
