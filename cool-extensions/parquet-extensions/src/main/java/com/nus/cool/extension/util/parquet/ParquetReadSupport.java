package com.nus.cool.extension.util.parquet;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;

/**
 * Helper class to expose schema for comparing against supplied schema in COOL.
 */
public class ParquetReadSupport extends GroupReadSupport {
  private MessageType schema;

  public MessageType getSchema() {
    return this.schema;
  }

  @Override
  public ReadContext init(Configuration configuration, 
    Map<String, String> keyValueMetaData, MessageType fileSchema) {
    this.schema = fileSchema;
    String partialSchemaString = configuration.get(
      ReadSupport.PARQUET_READ_SCHEMA);
    MessageType requestedProjection = getSchemaForRead(fileSchema,
      partialSchemaString);
    return new ReadContext(requestedProjection);
  }
}
