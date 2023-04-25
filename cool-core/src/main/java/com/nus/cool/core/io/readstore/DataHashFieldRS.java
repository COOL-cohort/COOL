package com.nus.cool.core.io.readstore;

import com.nus.cool.core.field.IntRangeField;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.io.storevector.IntFieldInputVector;
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;

/**
 * HashField ReadStore.
 */
public class DataHashFieldRS implements FieldRS {

  private FieldType fieldType;

  private InputVector<Integer> valueVector;

  // no used in true logic, to keep compatiable with old version code
  private IntFieldInputVector keyVector;

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
    // this.keyVector = InputVectorFactory.readFrom(buf);

    // this.valueVector = InputVectorFactory.readFrom(buf);

    this.keyVector = InputVectorFactory.genIntFieldInputVector(buffer);
    this.valueVector = InputVectorFactory.genIntInputVector(buffer);
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
  public IntRangeField getValueByIndex(int idx) {
    return this.keyVector.getValue(this.valueVector.get(idx));
  }


  // // Methods to keep compatiablity with old version code

  // @Override
  // public InputVector getKeyVector() {
  //   // TODO Auto-generated method stub
  //   return this.keyVector;
  // }

  // @Override
  // public InputVector getValueVector() {
  //   // TODO Auto-generated method stub
  //   return this.valueVector;
  // }

  // @Override
  // public int minKey() {
  //   // TODO Auto-generated method stub
  //   return 0;
  // }

  // @Override
  // public int maxKey() {
  //   return this.valueVector.size();
  // }

}
