package com.nus.cool.extension.util.reader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.reader.TupleReader;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AvroTupleReader implements TupleReader {

  private Schema srcSchema;
  private DataFileReader<GenericRecord> reader;
  private GenericRecord record;
  
  public AvroTupleReader(File avroData, Schema avroSchema) throws IOException {
    this.srcSchema = avroSchema;
    this.reader = new DataFileReader<GenericRecord>(avroData, 
      new GenericDatumReader<GenericRecord>(this.srcSchema));
  }

  /**
   * check whether all fields specified in the COOL schema 
   *  are in Parquet file schema
   * @param requestedSchema input schema of COOL
   * @return false if at least on field needed by COOL is not found 
   *  in Parquet data file; true otherwise.
   */
  public boolean checkSchemaCompatibility(TableSchema requestedSchema) {
    return requestedSchema.getFields().stream().allMatch(x -> this.srcSchema.getFields().stream().map(y -> y.name()).anyMatch(y -> y.equals(x.getName())));
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public Object next() throws IOException {
    record = reader.next(record);
    return record;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
