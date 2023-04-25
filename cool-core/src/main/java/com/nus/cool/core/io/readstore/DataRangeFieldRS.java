package com.nus.cool.core.io.readstore;

import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.io.storevector.RangeFieldInputVector;
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;


/** 
 *  RangeField ReadStore.
*/
public class DataRangeFieldRS implements FieldRS {

  private FieldType fieldType;

  private RangeField minKey;
  private RangeField maxKey;

  private RangeFieldInputVector valueVector;

  /**
   * static create function.
   *
   * @param buf memory
   * @param ft  fieldtype
   * @return DataRangeFieldRS
   */
  public static DataRangeFieldRS readFrom(ByteBuffer buf, FieldType ft) {
    DataRangeFieldRS instance = new DataRangeFieldRS();
    instance.readFromWithFieldType(buf, ft);
    return instance;
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    FieldType ft = FieldType.fromInteger(buffer.get());
    this.readFromWithFieldType(buffer, ft);
  }

  private void readFromWithFieldType(ByteBuffer buf, FieldType ft) {
    // get codec (no used)
    buf.get();
    switch (ft) {
      case Metric:
      case ActionTime:
        this.minKey = ValueWrapper.of(buf.getInt());
        this.maxKey = ValueWrapper.of(buf.getInt());
        break;
      case Float:
        this.minKey = ValueWrapper.of(buf.getFloat());
        this.maxKey = ValueWrapper.of(buf.getFloat());
        break;
      default:
        throw new IllegalArgumentException("Unexpected FieldType: " + ft);
    }
    this.fieldType = ft;
    this.valueVector = InputVectorFactory.genRangeFieldInputVector(buf);
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  // @Override
  public RangeField minKey() {
    return this.minKey;
  }

  // @Override
  public RangeField maxKey() {
    return this.maxKey;
  }

  @Override
  public RangeField getValueByIndex(int idx) {
    return this.valueVector.getValue(idx);
  }

}
