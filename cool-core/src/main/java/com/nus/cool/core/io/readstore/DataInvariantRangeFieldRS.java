package com.nus.cool.core.io.readstore;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
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

  private final int minKey;
  private final int maxKey;

  /**
   * Constructor of DataInvariantRangeFieldRS.
   *
   * @param fieldType     fieldType
   * @param invariantIdx  the index of this invariant schema
   * @param userMetaField userMetaField
   * @param userDataField userDataField
   */
  public DataInvariantRangeFieldRS(ByteBuffer buf, FieldType fieldType, int invariantIdx,
      MetaUserFieldRS userMetaField, DataHashFieldRS userDataField) {

    // get codec (no used)
    buf.get();
    this.minKey = buf.getInt();
    this.maxKey = buf.getInt();

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
    return this.minKey;
  }

  @Override
  public int maxKey() {
    return this.maxKey;
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    // TODO Auto-generated method stub

  }

}
