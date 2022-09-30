package com.nus.cool.core.io.writestore;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.converter.DayIntConverter;

public class MetaUserFieldWS extends MetaHashFieldWS {

    protected MetaChunkWS metaChunkWS;

    // key is the idx of invariant field
    protected final Map<Integer, List<Integer>> invariantIdxToValueList = Maps.newHashMap();
    

    public MetaUserFieldWS(FieldType type, Charset charset, OutputCompressor compressor, MetaChunkWS metaChunkWS) {
        super(type, charset, compressor);
        this.metaChunkWS = metaChunkWS;
        for (int invariant_idx : metaChunkWS.getTableSchema().getInvariantFieldIdxs()) {
            this.invariantIdxToValueList.put(invariant_idx, new ArrayList<>());
        }
    }

    @Override
    public void put(String[] tuple, int idx) {
        int hashKey = rhash.hash(tuple[idx]);
        if (this.fingerToGid.containsKey(hashKey)) {
            // this key is existed, skip
            return;
        }

        // the Gid is the index of its coressponding value in valueList
        this.fingerToGid.put(hashKey, nextGid++);
        this.valueList.add(tuple[idx]);
        
        // register the invariant field gid
        int[] invariant_idx_list = this.metaChunkWS.getTableSchema().getInvariantFieldIdxs();
        for (int invariant_idx : invariant_idx_list) {
            FieldSchema schema = this.metaChunkWS.getTableSchema().getField(invariant_idx);
            String invariant_v = tuple[invariant_idx];
            if (FieldType.isHashType(schema.getFieldType())) {
                // if invariant field is set type
                // we should convert the invariant String to invariant gid
                MetaFieldWS mws = this.metaChunkWS.getMetaFields()[invariant_idx];
                int invariant_gid = mws.find(tuple[invariant_idx]);
                this.invariantIdxToValueList.get(invariant_idx).add(invariant_gid);
            } else {
                // if invariant field is not set, directly store it
                if (schema.getFieldType() == FieldType.ActionTime) {
                    int v = DayIntConverter.getInstance().toInt(invariant_v);
                    this.invariantIdxToValueList.get(invariant_idx).add(v);
                } else {
                    int v = Integer.parseInt(invariant_v);
                    this.invariantIdxToValueList.get(invariant_idx).add(v);
                }
            }
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
    }

    @Override
    public int writeTo(DataOutput out) throws IOException {

        int bytesWritten = 0;

        // hashValue of userKey
        int[] fingers = new int[this.fingerToGid.size()];
        int[] globalIds = new int[this.fingerToGid.size()];
        int idx = 0;
        for (Map.Entry<Integer, Integer> en : this.fingerToGid.entrySet()) {
            globalIds[idx] = en.getValue();
            fingers[idx] = en.getKey();
            idx += 1;
        }

        // store fingers
        Histogram hist = Histogram.builder()
                .min(fingers[0])
                .max(fingers[fingers.length - 1])
                .sorted(true)
                .numOfValues(fingers.length)
                .rawSize(Ints.BYTES * fingers.length)
                .type(CompressType.KeyFinger)
                .build();
        this.compressor.reset(hist, fingers, 0, fingers.length);

        bytesWritten += this.compressor.writeTo(out);

        // save globalIds
        hist = Histogram.builder()
                .min(0)
                .max(this.fingerToGid.size())
                .numOfValues(globalIds.length)
                .rawSize(Ints.BYTES * globalIds.length)
                .type(CompressType.Value)
                .build();
        this.compressor.reset(hist, globalIds, 0, globalIds.length);
        bytesWritten += this.compressor.writeTo(out);

        TableSchema tableSchema = this.metaChunkWS.getTableSchema();
        // write invariant value
        int[] invariant_idx_list = tableSchema.getInvariantFieldIdxs();
        for (int invariant_idx : invariant_idx_list) {
            List<Integer> values = this.invariantIdxToValueList.get(invariant_idx);
            int max = this.nextGid;
            if(!FieldType.isHashType(tableSchema.getFieldType(invariant_idx))){
                // for matric type, get min, max from thier metaField

                // code is not good enough in it
                MetaRangeFieldWS ms = (MetaRangeFieldWS)this.metaChunkWS.getMetaFields()[invariant_idx];
                max = ms.getMax();
            }

            hist = Histogram.builder()
                .min(0)
                .max(max)
                .numOfValues(values.size())
                .rawSize(Ints.BYTES * globalIds.length)
                .sorted(false)
                .type(CompressType.Value)
                .build();
            int[] valueArray = Ints.toArray(values);
            this.compressor.reset(hist, valueArray, 0, values.size());
            bytesWritten += this.compressor.writeTo(out);
        }

        // Write value
        try(DataOutputBuffer buffer = new DataOutputBuffer()){
            buffer.writeInt(this.fingerToGid.size());
            int off = 0;
            // write the position
            for(String value: this.valueList){
                buffer.writeInt(off);
                off+=value.getBytes(this.charset).length;
            }

            for(String value: this.valueList){
                buffer.write(value.getBytes(this.charset));
            }

            hist = Histogram.builder()
                .type(CompressType.KeyString)
                .rawSize(buffer.size())
                .build();
            
            this.compressor.reset(hist, buffer.getData(), 0, buffer.size());
            bytesWritten += this.compressor.writeTo(out);
        }
        
        return bytesWritten;
    }

}