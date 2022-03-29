package com.nus.cool.core.util.parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.nus.cool.core.schema.TableSchema;

import org.testng.Assert;
import org.testng.annotations.Test;

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
        + "\"eventDay\":\"2013-05-21\","
        + "\"event\":\"fight\","
        + "\"money\":\"1638\","
        + "\"other\":\"Int96Value{Binary{3 constant bytes, [0, 0, 0]}}\"}";
      JsonStringTupleParser parser = new JsonStringTupleParser(schema);
      String[] ret = parser.parse(recordToParse);
      Assert.assertEquals(ret[0], "fd1ec667");
      Assert.assertEquals(ret[1], "43e3e0d84da1056");
      Assert.assertEquals(ret[2], "stonegolem");
      Assert.assertEquals(ret[3], "1638");
      Assert.assertEquals(ret[4], "fight");
      Assert.assertEquals(ret[5], "2013-05-21");
      Assert.assertEquals(ret[6],
        "Int96Value{Binary{3 constant bytes, [0, 0, 0]}}");
    } catch (IOException e) {
      System.err.println(e.getStackTrace());
    }
  }

}
