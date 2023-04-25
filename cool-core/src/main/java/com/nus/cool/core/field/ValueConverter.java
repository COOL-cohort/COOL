package com.nus.cool.core.field;

import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.io.IOException;
import lombok.AllArgsConstructor;


/**
 * Value converter from string array to FieldValue array based on the table schema.
 */
@AllArgsConstructor
public class ValueConverter {
  private final TableSchema schema;
  private final ValueConverterConfig config;

  private FieldValue convert(FieldType t, String v) throws IOException {
    switch (t) {
      case AppKey:
      case UserKey:
      case Action:
      case Segment:
        return new StringHashField(v);
      case Metric:
        return new IntRangeField(Integer.parseInt(v));
      case Float:
        return new FloatRangeField(Float.parseFloat(v));
      case ActionTime:
        return new IntRangeField(config.actionTimeIntConverter.toInt(v));
      default:
        throw new IOException("unexpected field value type.");
    }
  }

  /**
   * Convert a tuple to be represented by a FieldValue array.
   */
  public FieldValue[] convert(String[] tuple) throws IOException {
    if (tuple.length != schema.count()) {
      throw new IOException("Mismatch of field count of tuple with table schema");
    }
    
    FieldValue[] out = new FieldValue[tuple.length];
    for (int i = 0; i < tuple.length; i++) {
      out[i] = convert(schema.getFieldType(i), tuple[i]);
    }
    return out;
  }
}
