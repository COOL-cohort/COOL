package com.nus.cool.extension.util.reader;

import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.reader.TupleReader;
import com.nus.cool.extension.util.parquet.ParquetReadSupport;
import com.nus.cool.extension.util.parquet.ParquetRecordConverter;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.MessageType;


/**
 * This reader encapsulates the ParquetReader provided by Apache Parquet,
 *  and emit a record in a Json String to be interpreted by JsonTupleParser.
 *  Currently, we do not expose ParquetReader configuration.
 */
public class ParquetTupleReader implements TupleReader {
  private ParquetReader<Group> reader;
  private MessageType srcSchema;
  // private TableSchema requestedSchema;
  // [TBD] we could add a requested schema to filter out unnecessary column accesses.
  private Optional<String> record;

  /**
   * Create a reader for parquet data.
   */
  public ParquetTupleReader(String dataDirectory) throws IOException {
    ParquetReadSupport readSupport = new ParquetReadSupport();
    Path file = new Path(dataDirectory); 
    this.reader = ParquetReader.builder(readSupport, file).build();
    this.record = ParquetRecordConverter.convert(Optional.of(this.reader.read()));
    this.srcSchema = readSupport.getSchema();
  }

  /**
   * Check whether all fields specified in the COOL schema 
   *  are in Parquet file schema.
   *
   * @param requestedSchema input schema of COOL
   * @return false if at least on field needed by COOL is not found 
   *       in Parquet data file; true otherwise.
   */
  public boolean checkSchemaCompatibility(TableSchema requestedSchema) {
    return requestedSchema.getFields().stream().allMatch(
      x -> this.srcSchema.containsField(x.getName())
    );
  }


  /**
   * Check whether the reader finishes reading.
   *
   * @return 1 denotes the reader still needs to read and
   *      0 denoets there is nothing left to read.
   */
  @Override
  public boolean hasNext() {
    return record.isPresent();
  }

  /**
   * Get the next line of file.
   *
   * @return the original line
   */
  @Override
  public Object next() throws IOException {
    String old = this.record.orElse("");
    this.record = ParquetRecordConverter.convert(Optional.ofNullable(this.reader.read()));
    return old;
  }

  /**
   * Close the read.
   */
  @Override
  public void close() throws IOException {
    this.reader.close();
  }
}
