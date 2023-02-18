package com.nus.cool.core.io.writestore;

import com.google.common.collect.Maps;
import com.nus.cool.core.field.IntRangeField;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.converter.DayIntConverter;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * MetaField WriteStore for UserKey.
 */
public class MetaUserFieldWS extends MetaHashFieldWS {

  protected MetaChunkWS metaChunkWS;

  // key is the idx of invariant field
  protected final Map<Integer, List<IntRangeField>> invariantIdxToValueList = Maps.newHashMap();

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
    for (int invariantIdx : metaChunkWS.getTableSchema().getInvariantFieldIdxs()) {
      this.invariantIdxToValueList.put(invariantIdx, new LinkedList<>());
    }
  }

  @Override
  public void put(String[] tuple, int idx) {
    int hashKey = rhash.hash(tuple[idx]);
    if (this.fingerToGid.containsKey(hashKey)) {
      // skip existing key
      return;
    }

    // the Gid is the index of its coressponding value in valueList
    this.fingerToGid.put(hashKey, nextGid++);
    this.valueList.add(ValueWrapper.of(tuple[idx]));

    // register the invariant field gid
    int[] invariantIdxList = this.metaChunkWS.getTableSchema().getInvariantFieldIdxs();
    for (int invariantIdx : invariantIdxList) {
      FieldSchema schema = this.metaChunkWS.getTableSchema().getField(invariantIdx);
      String invariantV = tuple[invariantIdx];
      IntRangeField v;
      if (FieldType.isHashType(schema.getFieldType())) {
        // if invariant field is set type
        // we should convert the invariant String to invariant gid
        MetaFieldWS mws = this.metaChunkWS.getMetaFields()[invariantIdx];
        int invariantGid = mws.find(tuple[invariantIdx]);
        v = ValueWrapper.of(invariantGid);
      } else {
        // if invariant field is not set, directly store it
        if (schema.getFieldType() == FieldType.ActionTime) {
          v = ValueWrapper.of(DayIntConverter.getInstance().toInt(invariantV));
        } else {
          v = ValueWrapper.of(Integer.parseInt(invariantV));
        }
      }
      this.invariantIdxToValueList.get(invariantIdx).add(v);
    }
  }

  @Override
  public int count() {
    return this.fingerToGid.size();
  }

  @Override
  public void cleanForNextCublet() {
    this.fingerToGid.clear();
    this.invariantIdxToValueList.clear();
    this.valueList.clear();
    this.nextGid = 0; // a field can have different id across cublet.
  }

  @Override
  public int writeTo(DataOutput out) throws IOException {

    int bytesWritten = writeFingersAndGids(out);

    TableSchema tableSchema = this.metaChunkWS.getTableSchema();
    // write invariant value
    int[] invariantIdxList = tableSchema.getInvariantFieldIdxs();
    for (int invariantIdx : invariantIdxList) {
      List<IntRangeField> values = this.invariantIdxToValueList.get(invariantIdx);
      int max = this.nextGid;
      if (!FieldType.isHashType(tableSchema.getFieldType(invariantIdx))) {
        // for matric type, get min, max from thier metaField

        // code is not good enough in it
        MetaRangeFieldWS ms = (MetaRangeFieldWS) this.metaChunkWS.getMetaFields()[invariantIdx];
        max = ms.getMax();
      }

      Histogram hist = Histogram.builder()
          .sorted(false)
          .min(ValueWrapper.of(0))
          .max(ValueWrapper.of(max))
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