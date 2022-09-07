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

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldType;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User MetaField write store
 * <p>
 * User MetaField layout
 * -------------------------------------------------------------------
 * | finger codec | fingers | mapping of invariant data | value data |
 * -------------------------------------------------------------------
 * <p>
 * invariant data layout
 * -------------------------------------------------------------
 * | invariant field 1 | invariant field 2 | invariant field 3 |
 * -------------------------------------------------------------
 * <p>
 * value data layout
 * --------------------------------------------------
 * | #values | value codec | value offsets | values |
 * --------------------------------------------------
 */
public class MetaUserFieldWS extends MetaHashFieldWS {

    private Map<Integer, List<Object>> userToInvariant = Maps.newTreeMap();
    private int invatiantSize;

    public MetaUserFieldWS(FieldType type, Charset charset, OutputCompressor compressor, int invatiantSize) {
        super(type, charset, compressor);
        this.invatiantSize=invatiantSize;
    }

    public void put(String[] tupleValue, List<FieldType> invariantType) {
        int hashKey = rhash.hash(tupleValue[0]);
        if (!this.hashToTerm.containsKey(hashKey)) {
            this.hashToTerm.put(hashKey, new Term(tupleValue[0], nextGid++));
            this.gidToHash.add(hashKey);
        }
        if (tupleValue.length == 1) return;
        else {
            this.userToInvariant.put(hashKey, new ArrayList<>());
            int invariantSize = tupleValue.length - 1;
            for (int i = 0; i < invariantSize; i++) {
                if (invariantType.get(i) == FieldType.ActionTime || invariantType.get(i) == FieldType.Metric) {
                    this.userToInvariant.get(hashKey).add(Integer.parseInt(tupleValue[i + 1]));
                } else {
                    int invariantHashKey = rhash.hash(tupleValue[i + 1]);
                    this.userToInvariant.get(hashKey).add(invariantHashKey);
                }
            }
        }
    }

    public int writeTo(DataOutput out) throws IOException {
        int bytesWritten = 0;

        // Write fingers, i.e., the hash values of the original string, into the array
        // fingers contain data's hash value
        int[] fingers = new int[this.hashToTerm.size()];
        // globalIDs contain the global ids in the hash order
        int[] globalIDs = new int[this.hashToTerm.size()];

        int i = 0;
        for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
            globalIDs[i] = en.getValue().globalId;
            fingers[i] = en.getKey();
            i++;
        }

        // generate finger bytes
        Histogram hist = Histogram.builder()
                .min(fingers[0])
                .max(fingers[fingers.length - 1])
                .numOfValues(fingers.length)
                .rawSize(Ints.BYTES * fingers.length)
                .type(CompressType.KeyFinger)
                .build();
        this.compressor.reset(hist, fingers, 0, fingers.length);
        // Compress and write the fingers
        // Codec is written internal
        // Actually data is not compressed here
        bytesWritten += this.compressor.writeTo(out);


        // generate globalID bytes
        hist = Histogram.builder()
                .min(0)
                .max(this.hashToTerm.size())
                .numOfValues(globalIDs.length)
                .rawSize(Ints.BYTES * globalIDs.length)
                .type(CompressType.Value)// choose value as it is used for hash field columns of global id.
                .build();
        this.compressor.reset(hist, globalIDs, 0, globalIDs.length);
        bytesWritten += this.compressor.writeTo(out);

        // mapping of user and it's invariant data
        for (int j = 0; j < this.invatiantSize; j++) {
            int[] userCorrespondingInvariant = new int[this.hashToTerm.size()];
            int index = 0;
            for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
                userCorrespondingInvariant[index++] = (int) this.userToInvariant.get(en.getKey()).get(j);
            }
            hist = Histogram.builder()
                    .min(0)
                    .max(this.hashToTerm.size())
                    .numOfValues(userCorrespondingInvariant.length)
                    .rawSize(Ints.BYTES * userCorrespondingInvariant.length)
                    .type(CompressType.KeyFinger)
                    .build();
            this.compressor.reset(hist, userCorrespondingInvariant, 0, userCorrespondingInvariant.length);
            bytesWritten += this.compressor.writeTo(out);
        }


        // Write values
        if (this.fieldType == FieldType.Segment || this.fieldType == FieldType.Action
                || this.fieldType == FieldType.UserKey) {
            try (DataOutputBuffer buffer = new DataOutputBuffer()) {
                buffer.writeInt(this.hashToTerm.size());
                // Store offsets into the buffer first
                // Value offsets begin with 0
                int off = 0;
                for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
                    buffer.writeInt(off);
                    off += en.getValue().term.getBytes(this.charset).length;
                }

                // Store String values into the buffer
                for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
                    buffer.write(en.getValue().term.getBytes(this.charset));
                }

                // Compress and write the buffer
                // The codec is written internal
                hist = Histogram.builder()
                        .type(CompressType.KeyString)
                        .rawSize(buffer.size())
                        .build();
                this.compressor.reset(hist, buffer.getData(), 0, buffer.size());
                bytesWritten += this.compressor.writeTo(out);
            }
        }
        return bytesWritten;
    }
}
