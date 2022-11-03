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

package com.nus.cool.core.iceberg.query;

import com.nus.cool.core.iceberg.aggregator.Aggregator;
import com.nus.cool.core.iceberg.aggregator.AggregatorFactory;
import com.nus.cool.core.iceberg.aggregator.AggregatorFactory.AggregatorType;
import com.nus.cool.core.iceberg.result.AggregatorResult;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.converter.DayIntConverter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.map.HashedMap;

public class IcebergAggregation {

    public enum GroupType {

        STRING,

        NUMERIC
    }

    private String timeRange;

    private Map<String, BitSet> group = new HashMap<>();

    private List<Map<String, BitSet>> groups = new ArrayList<>();

    private ChunkRS dataChunk;

    private MetaChunkRS metaChunk;

    private AggregatorFactory aggregatorFactory = new AggregatorFactory();

    private String getString(FieldRS field, MetaFieldRS metaField, int value, GroupType type) {
        switch (type) {
            case STRING: {
                InputVector key = field.getKeyVector();
                return metaField.getString(key.get(value)); // localID => globalId => the filed => string
            }
            case NUMERIC: {
                return String.valueOf(value);
            }
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Group by one column
     * @param field: grouped filed read form one dataChunk
     * @param bs bitSet, filtered by timeRange and previous
     * @param metaField grouped filed read form one metaChunk
     * @param type group by type
     */
    private void group(FieldRS field, BitSet bs, MetaFieldRS metaField, GroupType type) {
        long beg = System.currentTimeMillis();
        Map<String, BitSet> group = new HashMap<>();
        // localID: bitMap, multiple rows may have same localID
        Map<Integer, BitSet> id2Bs = new HashedMap();
        // get all local ids ( hash ), or value ( range )
        InputVector values = field.getValueVector();

        for (int i = 0; i < values.size(); i++) {
            int nextPos = bs.nextSetBit(i); // get all position set to be true.
            if (nextPos < 0) {
                break;
            }
            values.skipTo(nextPos);
            int id = values.next(); // get local id
            if (id2Bs.get(id) == null) {
                BitSet groupBs = new BitSet(bs.size()); // number of records
                groupBs.set(nextPos);
                id2Bs.put(id, groupBs);
            }
            // if another record matches the same globalID, set the record position (nextPos) to be true
            else {
                id2Bs.get(id).set(nextPos);
            }
            i = nextPos;
        }

        for (Map.Entry<Integer, BitSet> entry : id2Bs.entrySet()) {
            group.put(getString(field, metaField, entry.getKey(), type), entry.getValue());
        }
        this.groups.add(group);
        long end = System.currentTimeMillis();
        //System.out.println("group elapsed: " + (end - beg));
    }

    /**
     * Group by one column
     * @param field: grouped filed read form one dataChunk
     * @param bs bitSet, filtered by timeRange and previous
     * @param granularity when groupField is actionTime, group by granularity.
     */
    private void group(FieldRS field, BitSet bs, IcebergQuery.GranularityType granularity) {
        assert field.getFieldType() == FieldType.ActionTime;
        long beg = System.currentTimeMillis();
        // timeStr: bitMap, multiple rows may have same localID
        Map<String, BitSet> timeStr2Bs = new HashedMap();

        // get all local ids ( hash ), or value ( range )
        InputVector values = field.getValueVector();

        for (int i = 0; i < values.size(); i++) {
            int nextPos = bs.nextSetBit(i); // get all position set to be true.
            if (nextPos < 0) {
                break;
            }
            values.skipTo(nextPos);
            int dataInt = values.next(); // get nUMERIC its self

            DayIntConverter converter = DayIntConverter.getInstance();
            String dataStr = converter.getString(dataInt);
            // convert to month based string
            String[] parts = dataStr.split("-");
            String monStr;
            if (granularity == IcebergQuery.GranularityType.YEAR){
                monStr = parts[0];

            }else if (granularity == IcebergQuery.GranularityType.MONTH){
                monStr = String.join("-", parts[0], parts[1]);
            }else {
                monStr = dataStr;
            }

            if ( !timeStr2Bs.containsKey(monStr) ){
                BitSet groupBs = new BitSet(bs.size()); // number of records
                groupBs.set(nextPos);
                timeStr2Bs.put(monStr, groupBs);
            }else{
                timeStr2Bs.get(monStr).set(nextPos);
            }
            i = nextPos;
        }

        // all to group
        Map<String, BitSet> group = new HashMap<>(timeStr2Bs);

        this.groups.add(group);
        long end = System.currentTimeMillis();
        //System.out.println("group elapsed: " + (end - beg));
    }

    private boolean checkOperatorIllegal(FieldType fieldType, AggregatorType aggregatorType) {
        switch (aggregatorType) {
            case SUM:
            case AVERAGE:
            case MAX:
            case MIN: {
                if (!fieldType.equals(FieldType.Metric)) {
                    return false;
                }
            }
            case COUNT:
                return true;
            case DISTINCTCOUNT:
                return !fieldType.equals(FieldType.Metric);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void mergeGroups() {
        this.group = this.groups.get(0);
        for(int i = 1; i < this.groups.size(); i++) {
            Map<String, BitSet> next = this.groups.get(i);
            Map<String, BitSet> merged = new HashMap<>();
            for (Map.Entry<String, BitSet> entry : this.group.entrySet()) {
                for(Map.Entry<String, BitSet> nextEntry : next.entrySet()) {
                    String groupName = entry.getKey() + "|" + nextEntry.getKey();
                    BitSet bs = (BitSet) entry.getValue().clone();
                    bs.and(nextEntry.getValue());
                    merged.put(groupName, bs);
                }
            }
            this.group = merged;
        }
    }

    /**
     * groupBy aggregation instance
     * @param bs bitMap, true means the record in time range
     * @param groupbyFields filed ot be group by
     * @param metaChunk current metaChunk
     * @param dataChunk current metaChunk
     * @param timeRange time Range
     */
    public void groupBy(BitSet bs, List<String> groupbyFields, MetaChunkRS metaChunk,
                     ChunkRS dataChunk, String timeRange, IcebergQuery.GranularityType GroupFields_granularity) {

        System.out.println("Init OLAP query timeRanges, key = "+ timeRange +", matched records = "+ bs.cardinality() + ", total size of BS = "+ bs.size());

        this.timeRange = timeRange;
        this.dataChunk = dataChunk;
        this.metaChunk = metaChunk;
        if (groupbyFields == null) {
            this.group.put("all", bs);
            return;
        }
        // group By multiple fields,
        for(String groupbyField : groupbyFields) {
            MetaFieldRS metaField = metaChunk.getMetaField(groupbyField);
            FieldRS dataField = dataChunk.getField(groupbyField);
            switch (dataField.getFieldType()) {
                case UserKey:
                case Action:
                case Segment:
                    group(dataField, bs, metaField, GroupType.STRING);
                    break;
                case Metric:
                    group(dataField, bs, metaField, GroupType.NUMERIC);
                    break;
                case ActionTime:
                    group(dataField, bs, GroupFields_granularity);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupport field type: " + dataField.getFieldType());
            }
        }
        mergeGroups();
    }

    public List<BaseResult> process(Aggregation aggregation) {

        String fieldName = aggregation.getFieldName();
        FieldType fieldType = this.metaChunk.getMetaField(fieldName).getFieldType();;

        // store the result
        Map<String, AggregatorResult> resultMap = new HashMap<>();

        for (AggregatorType aggregatorType : aggregation.getOperators()) {
            if (!checkOperatorIllegal(fieldType, aggregatorType)) {
                throw new IllegalArgumentException(fieldName + " can not process " + aggregatorType);
            }
            // do the aggregation
            Aggregator aggregator = this.aggregatorFactory.create(aggregatorType);
            FieldRS field = this.dataChunk.getField(fieldName);
            aggregator.process(this.group, field, resultMap, this.metaChunk.getMetaField(fieldName));
        }

        List<BaseResult> results = new ArrayList<>();
        for (Map.Entry<String, AggregatorResult> entry : resultMap.entrySet()) {
            BaseResult result = new BaseResult();
            result.setTimeRange(this.timeRange);
            result.setFieldName(fieldName);
            result.setKey(entry.getKey());
            result.setAggregatorResult(entry.getValue());
            results.add(result);
        }
        return results;
    }
}