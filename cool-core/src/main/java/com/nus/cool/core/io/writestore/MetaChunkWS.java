/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.nus.cool.core.io.writestore;

import com.google.common.primitives.Ints;
import com.nus.cool.core.io.Output;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.*;
import com.nus.cool.core.util.IntegerUtil;
import lombok.Getter;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * MetaChunk write store
 * <p>
 * MetaChunk layout
 * ------------------------------------------
 * | MetaChunkData | header | header offset |
 * ------------------------------------------
 * <p>
 * MetaChunkData layout
 * -------------------------------------
 * | field 1 | field 2 | ... | field N |
 * -------------------------------------
 * +++++++++++++++++++++++++++++++++++++
 * -------------------------------------
 * invariant table
 * -------------------------------------
 * <p>
 * header layout
 * ---------------------------------------
 * | ChunkType | #fields | field offsets |
 * ---------------------------------------
 * where
 * ChunkType = ChunkType.META
 * #fields = number of fields
 */
public class MetaChunkWS implements Output {

    private int offset;

    @Getter
    private final MetaFieldWS[] metaFields;

    @Getter
    private final MetaInvariantFieldWS metaInvariantField;


    /**
     * Constructor
     *  @param offset     Offset in out stream
     * @param metaFields fields type for each file of the meta chunk
     * @param metaInvariantField
     */
    public MetaChunkWS(int offset, MetaFieldWS[] metaFields, MetaInvariantFieldWS metaInvariantField, boolean invariant) {
        this.metaFields = checkNotNull(metaFields);
        this.metaInvariantField = metaInvariantField;
        checkArgument(offset >= 0 && metaFields.length > 0);
        this.offset = offset;
    }

    /**
     * MetaChunkWS Build
     *
     * @param schema table schema created with table.yaml
     * @param offset Offset begin to write metaChunk.
     * @return MetaChunkWS instance
     */
    public static MetaChunkWS newMetaChunkWS(TableSchema schema, int offset) {
        OutputCompressor compressor = new OutputCompressor();
        Charset charset = Charset.forName(schema.getCharset());

        // n: it denotes the number of columns
        int metaFieldSize = schema.getFields().size();
        boolean flag = false;
        MetaFieldWS[] metaFields = new MetaFieldWS[metaFieldSize];
        List<Integer>invariantIndex=new ArrayList<>();
        List<DataType> dataType=new ArrayList<>();
        for (int i = 0, metaFieldIndex = 0; i < metaFields.length; i++) {
            FieldSchema fieldSchema = schema.getField(i);
            FieldType fieldType = fieldSchema.getFieldType();
            boolean invariant = fieldSchema.getInvariant();
            if (invariant == false) {
                switch (fieldType) {
                    case AppKey:
                    case UserKey:
                    case Action:
                    case Segment:
                        metaFields[metaFieldIndex] = new MetaHashFieldWS(fieldType, charset, compressor);
                        metaFieldIndex += 1;
                        break;
                    case ActionTime:
                    case Metric:
                        metaFields[metaFieldIndex] = new MetaRangeFieldWS(fieldType);
                        metaFieldIndex += 1;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid field type: " + fieldType);
                }
            } else {
                flag = true;
                invariantIndex.add(i);
                dataType.add(fieldSchema.getDataType());
            }
        }
        Integer userKeyIndex=schema.getInvariantAppKeyField();
        return new MetaChunkWS(offset, metaFields, new MetaInvariantFieldWS(invariantIndex, userKeyIndex), flag);
    }

    /**
     * Put a tuple into the meta chunk
     *
     * @param tuple Plain data line
     */
    public void put(String[] tuple) {
        checkNotNull(tuple);
        List<Integer>invariantIndex=metaInvariantField.getInvariantIndex();
        List<Object> invariantData=new ArrayList<>();
        for (int i = 0, k=0; i < tuple.length; i++) {
            if(invariantIndex.contains(i)){
                invariantData.add(tuple[i]);
            }
            else {
                this.metaFields[k].put(tuple[i]);
                k++;
            }
        }
        boolean flag=invariantData.size()>0?true:false;
        if(flag){
            String userID=tuple[this.metaInvariantField.getUserKeyIndex()];
            this.metaInvariantField.putInvariant(userID,invariantData);
        }
    }

    /**
     * Complete MetaFields
     */
    public void complete() {
        for (MetaFieldWS metaField : this.metaFields) {
            metaField.complete();
        }
    }

    /**
     * Write MetaChunkWS to out stream and return bytes written
     *
     * @param out stream can write data to output stream
     * @return How many bytes has been written
     * @throws IOException If an I/O error occurs
     */
    @Override
    public int writeTo(DataOutput out) throws IOException {
        int bytesWritten = 0;
        bytesWritten+=this.metaInvariantField.writeTo(out);
        this.offset+=bytesWritten;
        // Store field offsets and write MetaFields as MetaChunkData layout
        int[] offsets = new int[this.metaFields.length];
        for (int i = 0; i < this.metaFields.length; i++) {
            offsets[i] = this.offset + bytesWritten;
            bytesWritten += this.metaFields[i].writeTo(out);
        }

        // Store header offset for MetaChunk layout
        int headOffset = this.offset + bytesWritten;

        // 1. Write ChunkType for header layout
        out.writeByte(ChunkType.META.ordinal());
        bytesWritten++;

        // 2.1 Write fields for header layout
        out.writeInt(IntegerUtil.toNativeByteOrder(this.metaFields.length));
        bytesWritten += Ints.BYTES;

        // 2.2 Write field offsets for header layout
        for (int offset : offsets) {
            out.writeInt(IntegerUtil.toNativeByteOrder(offset));
            bytesWritten += Ints.BYTES;
        }

        // 3. Write header offset for MetaChunk layout
        out.writeInt(IntegerUtil.toNativeByteOrder(headOffset));
        bytesWritten += Ints.BYTES;
        return bytesWritten;
    }

    @Override
    public String toString() {
        return "MetaChunk: " + Arrays.asList(metaFields).stream()
                .map(Object::toString).reduce((x, y) -> x + ", " + y);
    }

    /**
     * Update beginning offset to write the
     *
     * @param newOffset: new offset to write metaChunk
     */
    public void updateBeginOffset(int newOffset) {
        this.offset = newOffset;
    }
}
