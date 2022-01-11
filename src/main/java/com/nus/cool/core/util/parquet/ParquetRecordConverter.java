package com.nus.cool.core.util.parquet;

import java.util.Optional;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;

/**
 * Helper class to convert a Parquet record (Group)
 *  to a string in Json format.
 */
public class ParquetRecordConverter {

  public static Optional<String> convert(Optional<Group> record) {
    return record.map( x -> {
      StringBuilder builder = new StringBuilder();
      int fieldIndex = 0;
      builder.append('{');
      for (Type field : x.getType().getFields()) {
        String name = field.getName();
        int repetition = x.getFieldRepetitionCount(fieldIndex);
        if (repetition > 1) builder.append("{");
        for (int index = 0; index < repetition; index++) {
          Object value = x.getValueToString(fieldIndex, index);
          builder.append((index != 0) ? ",\"" : "\"").append(name)
          .append("\":");
          if (value == null) {
            builder.append("NULL");
          } else if(value instanceof Group) {
            builder.append(convert(Optional.of((Group) value)).get());
          } else {
            builder.append("\""+value.toString()+"\"");
          }
        }
        if (repetition > 1) builder.append("}");
        builder.append(",");
        ++fieldIndex;
      }
      builder.setLength(Math.max(builder.length()-1, 0)); // remove last comma.
      builder.append('}');
      return builder.toString();
    });
  }
}
