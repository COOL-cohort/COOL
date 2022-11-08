package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;

/**
 * Invariant HashField ReadStore.
 */
public class DataInvariantFieldRS implements FieldRS {

  private final MetaUserFieldRS userMetaField;

  private final FieldRS userDataField;

  private final FieldType fieldType;

  private final int invariantIdx;

  /**
   * Construct a invariant hash field.
   */
  public DataInvariantFieldRS(FieldType fieldType, int invariantIdx, MetaUserFieldRS userMetaField,
      DataHashFieldRS userDataField) {
    this.fieldType = fieldType;
    this.userMetaField = userMetaField;
    this.userDataField = userDataField;
    this.invariantIdx = invariantIdx;
  }

  /**
   * Construct a invariant range field.
   */
  public DataInvariantFieldRS(FieldType fieldType, int invariantIdx, MetaUserFieldRS userMetaField,
      DataRangeFieldRS userDataField) {
    this.userMetaField = userMetaField;
    this.userDataField = userDataField;
    this.fieldType = fieldType;
    this.invariantIdx = invariantIdx;
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public int getValueByIndex(int idx) {
    int gidUserKey = this.userDataField.getValueByIndex(idx);
    return this.userMetaField.getInvaraintValue(this.invariantIdx, gidUserKey);
  }

  // -------------- below methods are not used in new Version -----------------
  @Override
  public void readFrom(ByteBuffer buffer) {
  }

  @Override
  public InputVector getKeyVector() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputVector getValueVector() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int minKey() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int maxKey() {
    // TODO Auto-generated method stub
    return 0;
  }

}
