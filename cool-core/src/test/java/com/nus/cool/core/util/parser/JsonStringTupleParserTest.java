package com.nus.cool.core.util.parser;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueConverter;
import com.nus.cool.core.field.ValueConverterConfig;
import com.nus.cool.core.schema.TableSchema;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Testing json string tuple parser.
 */
public class JsonStringTupleParserTest {

  @Test
  public void testParse() {
    String schemaString = "charset: \"UTF-8\"\n"
        + "fields:\n"
        + "- name: \"sessionId\"\n"
        + "  fieldType: \"AppKey\"\n"
        + "  preCal: false\n"
        + "- name: \"playerId\"\n"
        + "  fieldType: \"UserKey\"\n"
        + "  preCal: false\n"
        + "- name: \"role\"\n"
        + "  fieldType: \"Segment\"\n"
        + "  preCal: false\n"
        + "- name: \"money\"\n"
        + "  fieldType: \"Metric\"\n"
        + "  preCal: false\n"
        + "- name: \"event\"\n"
        + "  fieldType: \"Action\"\n"
        + "  preCal: false\n"
        + "- name: \"eventDay\"\n"
        + "  fieldType: \"ActionTime\"\n"
        + "  preCal: false\n"
        + "- name: \"other\"\n"
        + "  fieldType: \"Segment\"\n"
        + "  preCal: false\n";
    try {
      InputStream in = new ByteArrayInputStream(
          schemaString.getBytes("UTF-8"));
      TableSchema schema = TableSchema.read(in);
      String recordToParse = "{\"sessionId\":\"fd1ec667\","
          + "\"int64_field\":\"64\","
          + "\"role\":\"stonegolem\","
          + "\"playerId\":\"43e3e0d84da1056\","
          + "\"eventDay\":\"2013-05-21 00:00:00\","
          + "\"event\":\"fight\","
          + "\"money\":\"1638\","
          + "\"other\":\"Int96Value{Binary{3 constant bytes, [0, 0, 0]}}\"}";
      JsonStringTupleParser parser = new JsonStringTupleParser(schema,
          new ValueConverter(schema, new ValueConverterConfig()));
      FieldValue[] ret = parser.parse(recordToParse);
      Assert.assertEquals(ret[0].getString(), "fd1ec667");
      Assert.assertEquals(ret[1].getString(), "43e3e0d84da1056");
      Assert.assertEquals(ret[2].getString(), "stonegolem");
      Assert.assertEquals(ret[3].getString(), "1638");
      Assert.assertEquals(ret[4].getString(), "fight");
      Assert.assertEquals(ret[5].getString(), Integer.toString(15846 * 24 * 3600));
      Assert.assertEquals(ret[6].getString(),
          "Int96Value{Binary{3 constant bytes, [0, 0, 0]}}");
    } catch (IOException e) {
      System.err.println(e.getStackTrace());
    }
  }

}
