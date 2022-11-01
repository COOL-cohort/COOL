package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;
import java.nio.ByteBuffer;

/**
 * Invariant RangeField ReadStore.
 */
public class DataInvariantRangeFieldRS implements FieldRS {

  private final MetaUserFieldRS userMetaField;

  private final DataHashFieldRS userDataField;

  private final FieldType fieldType;

  private final int invariantIdx;

  /**
   * Constructor of DataInvariantRangeFieldRS.
   *
   * @param fieldType     fieldType
   * @param invariantIdx  the index of this invariant schema
   * @param userMetaField userMetaField
   * @param userDataField userDataField
   */
  public DataInvariantRangeFieldRS(FieldType fieldType, int invariantIdx,
      MetaUserFieldRS userMetaField, DataHashFieldRS userDataField) {
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
    int gidOfUserKey = this.userDataField.getValueByIndex(idx);
    return this.userMetaField.getInvaraintValue(this.invariantIdx, gidOfUserKey);
  }

  // -------------- above method is no used in new Version -----------------
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

  @Override
  public void readFrom(ByteBuffer buffer) {
    // TODO Auto-generated method stub

  }

}
