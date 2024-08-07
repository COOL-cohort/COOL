package com.nus.cool.core.io.writestore;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.HashField;
import com.nus.cool.core.field.IntRangeField;
import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * MetaField WriteStore for UserKey.
 */
public class MetaUserFieldWS extends MetaHashFieldWS {

  protected MetaChunkWS metaChunkWS;

  // key is the idx of invariant field
  protected final Map<Integer, List<RangeField>> invariantIdxToValueList = Maps.newHashMap();

  /**
   * Constructor for MetaUserFieldWS.
   *
   * @param type        type
   * @param charset     charset
   * @param metaChunkWS metaChunkWS
   */
  public MetaUserFieldWS(FieldType type, Charset charset, MetaChunkWS metaChunkWS) {
    super(type, charset);
    this.metaChunkWS = metaChunkWS;
    this.resetInvariantIdxToValueList();
  }

  private void resetInvariantIdxToValueList() {
    this.invariantIdxToValueList.clear();
    for (int invariantIdx : this.metaChunkWS.getTableSchema().getInvariantFieldIdxs()) {
      this.invariantIdxToValueList.put(invariantIdx, new LinkedList<>());
    }
  }

  @Override
  public void put(FieldValue[] tuple, int idx) throws IllegalArgumentException {
    if (!(tuple[idx] instanceof HashField)) {
      throw new IllegalArgumentException(
          "Illegal argument for MetaUserFieldWS (HashField required).");
    }
    HashField user = (HashField) tuple[idx];
    int hashKey = user.getInt();
    if (this.fingerToGid.containsKey(hashKey)) {
      // skip existing key
      return;
    }

    // the Gid is the index of its coressponding value in valueList
    this.fingerToGid.put(hashKey, nextGid++);
    this.valueList.add(user);

    // register the invariant field gid
    int[] invariantIdxList = this.metaChunkWS.getTableSchema().getInvariantFieldIdxs();
    for (int invariantIdx : invariantIdxList) {
      MetaFieldWS mws = this.metaChunkWS.getMetaFields()[invariantIdx];
      FieldValue v = tuple[invariantIdx];
      if (mws instanceof MetaHashFieldWS) {
        // if invariant field is set type
        // we should convert the invariant String to invariant gid
        if (!(v instanceof HashField)) {
          throw new IllegalArgumentException("Invalid argument (HashField required).");
        }
        int invariantGid = ((MetaHashFieldWS) mws).find((HashField) v);
        this.invariantIdxToValueList.get(invariantIdx).add(new IntRangeField(invariantGid));
      } else {
        if (!(v instanceof RangeField)) {
          throw new IllegalArgumentException("Invalid argument (RangeField required).");
        }
        this.invariantIdxToValueList.get(invariantIdx).add((RangeField) v);
      }
    }
  }

  @Override
  public int count() {
    return this.fingerToGid.size();
  }

  @Override
  public void cleanForNextCublet() {
    super.cleanForNextCublet();
    this.resetInvariantIdxToValueList();
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {
    int bytesWritten = writeFingersAndGids(out);

    TableSchema tableSchema = this.metaChunkWS.getTableSchema();
    // write invariant value
    int[] invariantIdxList = tableSchema.getInvariantFieldIdxs();
    for (int invariantIdx : invariantIdxList) {
      List<RangeField> values = this.invariantIdxToValueList.get(invariantIdx);
      RangeField max = new IntRangeField(this.nextGid);
      if (!FieldType.isHashType(tableSchema.getFieldType(invariantIdx))) {
        // for matric type, get min, max from thier metaField

        // code is not good enough in it
        MetaRangeFieldWS ms = (MetaRangeFieldWS) this.metaChunkWS.getMetaFields()[invariantIdx];
        max = ms.getMax();
      }

      Histogram hist = Histogram.builder()
          .sorted(false)
          .min(ValueWrapper.of(0))
          .max(max)
          .numOfValues(values.size())
          .build();
      bytesWritten += OutputCompressor.writeTo(CompressType.Value, hist,
          values, out);
    }

    // Write value
    bytesWritten += OutputCompressor.writeTo(CompressType.KeyString,
        Histogram.builder().charset(charset).build(),
        valueList, out);

    return bytesWritten;
  }

}