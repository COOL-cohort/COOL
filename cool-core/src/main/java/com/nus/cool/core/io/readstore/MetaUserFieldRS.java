package com.nus.cool.core.io.readstore;

import static com.google.common.base.Preconditions.checkNotNull;

import com.nus.cool.core.field.HashField;
import com.nus.cool.core.field.IntRangeField;
import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.io.storevector.HashFieldInputVector;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.io.storevector.IntFieldInputVector;
import com.nus.cool.core.schema.FieldType;
import com.rabinhash.RabinHashFunction32;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

/**
 * Meta UserField ReadStore.
 */
public class MetaUserFieldRS implements MetaFieldRS {

  protected static final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

  protected Charset charset;

  protected MetaChunkRS metaChunkRS;

  protected FieldType fieldType;

  protected InputVector<Integer> fingerVec;

  protected InputVector<Integer> globalIDVec;

  protected HashFieldInputVector valueVec;

  protected IntFieldInputVector[] invarantMaps;
  // the idx is the globalIdIdx, the value is the globalId of invariant field

  /**
   * Constructor of MetaUserFieldRS.
   *
   * @param metaChunkRS metaChunkRS to get UserMetaField
   * @param charset default utf-8
   */
  public MetaUserFieldRS(MetaChunkRS metaChunkRS, Charset charset) {
    this.charset = checkNotNull(charset);
    this.metaChunkRS = metaChunkRS;
    int invariantSize = metaChunkRS.getSchema().getInvariantFieldNumber();
    this.invarantMaps = new IntFieldInputVector[invariantSize];
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    FieldType fieldType = FieldType.fromInteger(buffer.get());
    this.readFromWithFieldType(buffer, fieldType);
  }

  @Override
  public FieldType getFieldType() {
    return this.fieldType;
  }

  @Override
  public int find(String key) {
    int globalIdIdx = this.fingerVec.find(rhash.hash(key));
    return this.globalIDVec.get(globalIdIdx);
  }

  @Override
  public int count() {
    return this.fingerVec.size();
  }

  @Override
  public Optional<HashField> get(int i) {
    // return ((LZ4InputVector) this.valueVec).getString(i, this.charset);
    // return this.valueVec.get(i);
    return (i < count()) ? Optional.of(this.valueVec.getValue(i)) : Optional.empty();
  }

  @Override
  public RangeField getMaxValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RangeField getMinValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType) {
    this.fieldType = fieldType;
    this.fingerVec = InputVectorFactory.genIntInputVector(buffer);
    this.globalIDVec = InputVectorFactory.genIntInputVector(buffer);
    for (int i = 0; i < this.invarantMaps.length; i++) {
      this.invarantMaps[i] = InputVectorFactory.genIntFieldInputVector(buffer);
    }
    this.valueVec = InputVectorFactory.genHashFieldInputVector(buffer, charset);
  }

  // --------------- specific method for MetaUserField ---------------

  /**
   * Get invariant Value.
   *
   * @param invariantIdx the index of invariant field in all invariant fields
   * @param gid          the according gloablId of UserKey
   * @return int globalId
   */
  public IntRangeField getInvaraintValue(int invariantIdx, int gid) {
    return this.invarantMaps[invariantIdx].getValue(gid);
  }

}
