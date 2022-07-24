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
import com.nus.cool.core.schema.TableSchema;
import com.rabinhash.RabinHashFunction32;
import lombok.Getter;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Hash MetaField write store
 * <p>
 * Hash MetaField layout
 * ---------------------------------------
 * | finger codec | fingers | value data |
 * ---------------------------------------
 * <p>
 * if fieldType == AppId, we do NOT store values
 * <p>
 * value data layout
 * --------------------------------------------------
 * | #values | value codec | value offsets | values |
 * --------------------------------------------------
 */
public class MetaHashFieldWS implements MetaFieldWS {

    private final Charset charset;
    private final FieldType fieldType;
    private final OutputCompressor compressor;
    private final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

    /**
     * Global hashToTerm, keys are hashed by the indexed string
     */

    // hash of one tuple field : Term {origin value of tuple filed, global ID. }
    private final Map<Integer, Term> hashToTerm = Maps.newTreeMap();

    // store keys of hashToTerm
    private final List<Integer> gidToHash = new ArrayList<>();

    //    private final List<Map<Integer, Term>> invariantHashToTerm = new ArrayList<>();
//    private final List<List<Integer>> invariantGidToHash = new ArrayList<>();
    private Map<Integer, List<Object>> userToInvariant = Maps.newTreeMap();
    // global id
    private int nextGid = 0;
//    private List<Integer> invariantNextGid = new ArrayList<>();

    @Getter
    private Map<String, Integer> invariantName2Id;

    public MetaHashFieldWS(FieldType type, Charset charset, OutputCompressor compressor) {
        this.fieldType = type;
        this.charset = charset;
        this.compressor = compressor;
    }

    public MetaHashFieldWS(FieldType type, Charset charset, OutputCompressor compressor, TableSchema schema) {
        this.fieldType = type;
        this.charset = charset;
        this.compressor = compressor;
        this.invariantName2Id = schema.getInvariantName2Id();
    }

    @Override
    public void put(String tupleValue) {
        int hashKey = rhash.hash(tupleValue);
        if (!this.hashToTerm.containsKey(hashKey)) {
            this.hashToTerm.put(hashKey, new Term(tupleValue, nextGid++));
            this.gidToHash.add(hashKey);
        }
    }

    @Override
    public void putUser(String[] tupleValue, List<FieldType> invariantType) {
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
//                if (this.invariantHashToTerm.size() < (i + 1)) {
//                    this.invariantHashToTerm.add(Maps.newTreeMap());
//                    this.invariantGidToHash.add(new ArrayList<>());
//                    this.invariantNextGid.add(0);
//
//                    this.invariantHashToTerm.get(i).put(invariantHashKey, new Term(tupleValue[i + 1], this.invariantNextGid.get(i)));
//                    this.invariantNextGid.set(i, this.invariantNextGid.get(i) + 1);
//                    this.invariantGidToHash.get(i).add(invariantHashKey);
//                } else {
//                    if (!this.invariantHashToTerm.get(i).containsKey(invariantHashKey)) {
//                        this.invariantHashToTerm.get(i).put(invariantHashKey, new Term(tupleValue[i + 1], this.invariantNextGid.get(i)));
//                        this.invariantNextGid.set(i, this.invariantNextGid.get(i) + 1);
//                        this.invariantGidToHash.get(i).add(invariantHashKey);
//                    }
//                }


            }
        }
    }

    @Override
    public int find(String v) {
        // TODO: Need to handle the case where v is null
        int fp = this.rhash.hash(v);
        return this.hashToTerm.containsKey(fp) ? this.hashToTerm.get(fp).globalId : -1;
    }

    @Override
    public int count() {
        return this.hashToTerm.size();
    }

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public void complete() {
        int gID = 0;
        // Set globalIDs
        for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
            // temp fix to not delete complete logic, while preserving correctness of using update.
            if (en.getValue().globalId == 0) en.getValue().globalId = gID++;
        }
    }

    @Override
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
            fingers[i++] = en.getKey();
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

        // Write values
        if (this.fieldType == FieldType.Segment || this.fieldType == FieldType.Action
                || this.fieldType == FieldType.UserKey) {
            try (DataOutputBuffer buffer = new DataOutputBuffer()) {
                // Store offsets into the buffer first
                buffer.writeInt(this.hashToTerm.size());
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

    @Override
    public int writeUserTo(DataOutput out) throws IOException {
        int bytesWritten = 0;

        // Write fingers, i.e., the hash values of the original string, into the array
        // fingers contain data's hash value
        int[] fingers = new int[this.hashToTerm.size()];
        int[] sortedFingers=new int[this.hashToTerm.size()];
        // globalIDs contain the global ids in the hash order
        int[] globalIDs = new int[this.hashToTerm.size()];
        int[] sortedGlobalIDs=new int[this.hashToTerm.size()];

        int i = 0;
        for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
            globalIDs[i] = en.getValue().globalId;
            fingers[i] = en.getKey();
            i++;
        }

        Comparator<Map.Entry<Integer, Term>> valueComparator
                = new Comparator<Map.Entry<Integer, Term>>() {
            @Override
            public int compare(Map.Entry<Integer, Term> e1, Map.Entry<Integer, Term> e2) {
                Integer v1 = e1.getValue().globalId;
                Integer v2 = e2.getValue().globalId;
                return v1.compareTo(v2);
            }
        };

        List<Map.Entry<Integer, Term>> listOfEntries
                = new ArrayList<Map.Entry<Integer, Term>>(this.hashToTerm.entrySet());
        Collections.sort(listOfEntries, valueComparator);

        LinkedHashMap<Integer, Term> sortedByValue
                = new LinkedHashMap<Integer, Term>(listOfEntries.size());
        for(Map.Entry<Integer, Term> entry :listOfEntries)
        {
            sortedByValue.put(entry.getKey(), entry.getValue());
        }
        i=0;
        for (Map.Entry<Integer, Term> en : sortedByValue.entrySet()) {
            sortedGlobalIDs[i] = en.getValue().globalId;
            sortedFingers[i++] = en.getKey();
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

        hist = Histogram.builder()
                .min(fingers[0])
                .max(fingers[fingers.length - 1])
                .numOfValues(fingers.length)
                .rawSize(Ints.BYTES * fingers.length)
                .type(CompressType.KeyFinger)
                .build();
        this.compressor.reset(hist, sortedFingers, 0, fingers.length);
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

        hist = Histogram.builder()
                .min(0)
                .max(this.hashToTerm.size())
                .numOfValues(globalIDs.length)
                .rawSize(Ints.BYTES * globalIDs.length)
                .type(CompressType.Value)// choose value as it is used for hash field columns of global id.
                .build();
        this.compressor.reset(hist, sortedGlobalIDs, 0, globalIDs.length);
        bytesWritten += this.compressor.writeTo(out);

        for (int j = 0; j < invariantName2Id.size(); j++) {
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
//                for (int j = 0; j < this.invariantHashToTerm.size(); j++) {
//                    buffer.writeInt(this.invariantHashToTerm.get(j).size());
//                }
                // Value offsets begin with 0
                int off = 0;
                for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
                    buffer.writeInt(off);
                    off += en.getValue().term.getBytes(this.charset).length;
                }
//                for (int j = 0; j < this.invariantHashToTerm.size(); j++) {
//                    for (Map.Entry<Integer, Term> en : this.invariantHashToTerm.get(j).entrySet()) {
//                        buffer.writeInt(off);
//                        off += this.invariantHashToTerm.get(j).get(this.userToInvariant.get(en.getKey()).get(j)).term.getBytes(this.charset).length;
//                    }
//                }

                // Store String values into the buffer
                for (Map.Entry<Integer, Term> en : this.hashToTerm.entrySet()) {
                    buffer.write(en.getValue().term.getBytes(this.charset));
//                    for (int j = 0; j < invariantName2Id.size(); j++) {
//                        buffer.write(this.invariantHashToTerm.get(j).get(this.userToInvariant.get(en.getKey()).get(j)).term.getBytes(this.charset));
//                    }
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

    @Override
    public String toString() {
        return "HashMetaField: " + hashToTerm.entrySet().stream().map(x -> x.getKey() + "-" + x.getValue()).collect(Collectors.toList());
    }

    @Override
    public void update(String tuple) {
        throw new UnsupportedOperationException("Doesn't support update now");
    }

    @Override
    public void putUser(String[] tupleValue) {

    }

    /**
     * Convert string to globalIDs
     */
    public static class Term {

        // the real value in each row of the csv file
        String term;
        // assigned global ID.
        int globalId;

        public Term(String term, int globalId) {
            this.term = term;
            this.globalId = globalId;
        }

        @Override
        public String toString() {
            return "{term: " + term + ", globalId: " + globalId + "}";
        }
    }
}
