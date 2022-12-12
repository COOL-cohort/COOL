package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;

/**
 * HashField ReadStore.
 */
public class DataHashFieldRS implements FieldRS {

  private FieldType fieldType;

  private InputVector valueVector;

  // no used in true logic, to keep compatiable with old version code
  private InputVector keyVector;

  /**
   * static create function.
   *
   * @param buffer memory
   * @param ft     fieldtype
   * @return DataHashFieldRS
   */
  public static DataHashFieldRS readFrom(ByteBuffer buffer, FieldType ft) {
    DataHashFieldRS instance = new DataHashFieldRS();
    instance.readFromWithFieldType(buffer, ft);
    return instance;
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    FieldType fieldType = FieldType.fromInteger(buffer.get());
    this.readFromWithFieldType(buffer, fieldType);
  }

  /**
   * initialized function.
   *
   * @param buffer memory
   * @param ft     fieldtype
   */
  public void readFromWithFieldType(ByteBuffer buffer, FieldType ft) {
    this.fieldType = ft;
    // global ids.
    this.keyVector = InputVectorFactory.readFrom(buffer);
    // local ids.
    this.valueVector = InputVectorFactory.readFrom(buffer);
  }

  /**
   * BitSet array if this field has been pre-calculated.
   */
  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  /**
   * Get the globalId by index.
   */
  @Override
  public int getValueByIndex(int idx) {
    return this.keyVector.get(this.valueVector.get(idx));
  }

  // Methods to keep compatiablity with old version code
  @Override
  public InputVector getKeyVector() {
    // TODO Auto-generated method stub
    return this.keyVector;
  }

  @Override
  public InputVector getValueVector() {
    // TODO Auto-generated method stub
    return this.valueVector;
  }

  @Override
  public int minKey() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int maxKey() {
    return this.valueVector.size();
  }

  @Override
  public int getFieldSize() {
    return this.valueVector.size();
  }

}
