package com.nus.cool.extension.util.config;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueConverterConfig;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.parser.TupleParser;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing avro data loader configuration.
 */
public class AvroDataLoaderConfigTest {
  @Test(dataProvider = "AvroSchemaParserDP")
  public void testCreateTupleParser(String tableSchemaString, String avroSchemaString)
      throws IOException {

    InputStream in = new ByteArrayInputStream(
            tableSchemaString.getBytes("UTF-8"));
    TableSchema schema = TableSchema.read(in);
    AvroDataLoaderConfig config = new AvroDataLoaderConfig();
    TupleParser avroParser = config.createTupleParser(schema, new ValueConverterConfig());

    //parse the schema
    Schema.Parser parser = new Schema.Parser();
    Schema avroSchema = parser.parse(avroSchemaString);
    //prepare the avro record
    GenericRecord avroRecord = new GenericData.Record(avroSchema);

    avroRecord.put("sessionId", "0001");
    avroRecord.put("playerId", "0008");
    avroRecord.put("role", 2);

    FieldValue[] parsed = avroParser.parse(avroRecord);
    Assert.assertEquals(parsed.length, 3);
    Assert.assertEquals(parsed[0].getString(), "0001");
    Assert.assertEquals(parsed[1].getString(), "0008");
    Assert.assertEquals(parsed[2].getString(), "2");
  }

  /**
   * Data provider for testing avro data loading.
   */
  @DataProvider(name = "AvroSchemaParserDP")
  public Object[][] dpArgs() {
    //table schema
    String tableSchemaString = "charset: \"UTF-8\"\n"
        + "fields:\n"
        + "- name: \"sessionId\"\n"
        + "  fieldType: \"AppKey\"\n"
        + "  preCal: false\n"
        + "- name: \"playerId\"\n"
        + "  fieldType: \"UserKey\"\n"
        + "  preCal: false\n"
        + "- name: \"role\"\n"
        + "  fieldType: \"Segment\"\n"
        + "  preCal: false\n";
    //avro schema
    String avroSchemaString = "{"
        + "   \"type\": \"record\","
        + "   \"name\": \"test\","
        + "   \"fields\": ["
        + "       {\"name\": \"sessionId\", \"type\": \"string\"},"
        + "       {\"name\": \"playerId\", \"type\": \"string\"},"
        + "       {\"name\": \"role\", \"type\": \"int\"}"
        + "   ]"
        + "}";

    return new Object[][]{
      {tableSchemaString, avroSchemaString}
    };
  }
}
