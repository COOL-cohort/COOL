package com.nus.cool.extension.util.parquet;

import java.util.Optional;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ParquetRecordConverterTest {
 
  @Test
  public void testConvert() {
    MessageType schema = MessageTypeParser.parseMessageType(
      "message test { "
      + "required binary binary_field; "
      + "required int32 int32_field; "
      + "required int64 int64_field; "
      + "required boolean boolean_field; "
      + "required float float_field; "
      + "required double double_field; "
      + "required fixed_len_byte_array(3) flba_field; "
      + "required int96 int96_field; "
      + "} ");
    Group record = new SimpleGroup(schema);
    record.append("binary_field", "test0")
          .append("int32_field", 32)
          .append("int64_field", 64l)
          .append("boolean_field", true)
          .append("float_field", 1.0f)
          .append("double_field", 2.0d)
          .append("flba_field", "foo")
          .append("int96_field", Binary.fromConstantByteArray(new byte[3]));
    String converted = ParquetRecordConverter.convert(
      Optional.of(record)).get();

    Assert.assertEquals(converted, "{\"binary_field\":\"test0\","
      + "\"int32_field\":\"32\",\"int64_field\":\"64\","
      + "\"boolean_field\":\"true\",\"float_field\":\"1.0\","
      + "\"double_field\":\"2.0\",\"flba_field\":\"foo\","
      + "\"int96_field\":\"Int96Value{Binary{3 constant bytes, [0, 0, 0]}}\"}"
    );
  }

}
