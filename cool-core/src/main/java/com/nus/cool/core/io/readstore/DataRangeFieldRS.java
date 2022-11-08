package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;


/** 
 *  RangeField ReadStore.
*/
public class DataRangeFieldRS implements FieldRS {

  private FieldType fieldType;

  private int minKey;
  private int maxKey;

  private InputVector valueVector;

  /**
   * static create function.
   *
   * @param buf memory
   * @param ft     fieldtype
   * @return DataRangeFieldRS
   */
  public static DataRangeFieldRS readFrom(ByteBuffer buf, FieldType ft) {
    DataRangeFieldRS instance = new DataRangeFieldRS();
    instance.readFromWithFieldType(buf, ft);
    return instance;
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    FieldType fieldType = FieldType.fromInteger(buffer.get());
    this.readFromWithFieldType(buffer, fieldType);
  }

  private void readFromWithFieldType(ByteBuffer buf, FieldType ft) {
    // get codec (no used)
    buf.get();
    this.fieldType = ft;
    this.minKey = buf.getInt();
    this.maxKey = buf.getInt();
    this.valueVector = InputVectorFactory.readFrom(buf);
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public int minKey() {
    return this.minKey;
  }

  @Override
  public int maxKey() {
    return this.maxKey;
  }

  @Override
  public int getValueByIndex(int idx) {
    return this.valueVector.get(idx);
  }


  // not used, only to keep compatiablity with old version code
  @Override
  public InputVector getKeyVector() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputVector getValueVector() {
    return this.valueVector;
  }

}
