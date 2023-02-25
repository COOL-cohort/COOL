package com.nus.cool.core.util.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueConverter;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.TableSchema;
import java.io.IOException;
import lombok.AllArgsConstructor;

/**
 * JsonStringTupleParser requires the schema as input,
 * because we do not make any assumption on the field order.
 */
@AllArgsConstructor
public class JsonStringTupleParser implements TupleParser {
  private TableSchema schema;

  private ValueConverter converter;

  @Override
  public FieldValue[] parse(Object tuple) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode in = mapper.readTree((String) tuple);
    int numFields = this.schema.getFields().size();
    String[] output = new String[numFields];
    int index = 0;
    for (FieldSchema field : this.schema.getFields()) {
      output[index] = in.get(field.getName()).toString();
      // remove excessive enclosing quotation marks.
      output[index] = output[index].substring(1,
          output[index].length() - 1);
      index++;
    }
    return converter.convert(output);
  }
}
