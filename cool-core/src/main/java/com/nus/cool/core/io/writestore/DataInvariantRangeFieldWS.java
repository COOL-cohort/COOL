package com.nus.cool.core.io.writestore;

import com.google.common.primitives.Ints;
import com.nus.cool.core.io.DataInputBuffer;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.IntegerUtil;
import com.nus.cool.core.util.converter.DayIntConverter;
import java.io.DataOutput;
import java.io.IOException;



/**
 * whether to record the field's meta data in this chunk (min,max).
 * it can speed up the cohort processing.
 * Data layout
 * ---------------------
 * | codec | min | max |
 * ---------------------
 * where
 * min = min of the values
 * max = max of the values
 */
public class DataInvariantRangeFieldWS implements DataFieldWS {

  private final FieldType fieldType;

  private final DataOutputBuffer buffer = new DataOutputBuffer();

  public DataInvariantRangeFieldWS(FieldType fieldType) {
    this.fieldType = fieldType;

  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public void put(String tupleValue) throws IOException {
    if (this.fieldType == FieldType.ActionTime) {
      DayIntConverter converter = DayIntConverter.getInstance();
      this.buffer.writeInt(converter.toInt(tupleValue));
    } else {
      this.buffer.writeInt(Integer.parseInt(tupleValue));
    }
  }

  /**
   * Store only min-max for speed up the query.
   *
   * @param out stream can write data to output stream.
   * @return number of Bytes used.
   * @throws IOException IOException.
   */
  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = 0;
    int[] key = { Integer.MAX_VALUE, Integer.MIN_VALUE };

    // Read column data
    try (DataInputBuffer input = new DataInputBuffer()) {
      input.reset(this.buffer);
      for (int i = 0; i < this.buffer.size() / Ints.BYTES; i++) {
        int value = input.readInt();
        key[0] = Math.min(value, key[0]);
        key[1] = Math.max(value, key[1]);
      }
    }

    // Write codec
    out.write(Codec.Range.ordinal());
    bytesWritten++;

    // Write max value
    out.writeInt(IntegerUtil.toNativeByteOrder(key[0]));
    bytesWritten += Ints.BYTES;
    // Write max value
    out.writeInt(IntegerUtil.toNativeByteOrder(key[1]));
    bytesWritten += Ints.BYTES;
    return bytesWritten;
  }
}
