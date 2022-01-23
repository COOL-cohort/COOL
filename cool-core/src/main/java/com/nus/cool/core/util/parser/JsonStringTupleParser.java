package com.nus.cool.core.util.parser;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.TableSchema;

/**
 * JsonStringTupleParser requires the schema as input,
 *  because we do not make any assumption on the field order. 
 */
public class JsonStringTupleParser implements TupleParser {
  private TableSchema schema;
  
  private static final String[] EMPTY_OUTPUT = {};

  public JsonStringTupleParser(TableSchema schema) {
    this.schema = schema;
  }
  
  @Override
  public String[] parse(Object tuple) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode in = mapper.readTree((String) tuple);
      int numFields = this.schema.getFields().size();
      String[] output = new String[numFields];
      int index = 0;
      for (FieldSchema field : this.schema.getFields()) {
        output[index] = in.get(field.getName()).toString();
        // remove excessive enclosing quotation marks.
        output[index] = output[index].substring(1,
          output[index].length()-1);
        index++;
      }
      return output;
    } catch (IOException e) { // superset of of JsonParseException and JsonProcessingException 
      System.err.println("Tuple to decode is not in Json format: "
        + e.getMessage() + "\n");
    }
    return EMPTY_OUTPUT;
  }
}
