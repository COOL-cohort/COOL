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

package com.nus.cool.core.cohort.olapselect;

import com.nus.cool.core.cohort.OLAPQueryLayout.GranularityType;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.converter.SecondIntConverter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

/**
 * OLAP aggregator and group by.
 */
public class OLAPGroupBy {

  /**
   * GroupBy's Type.
   */
  public enum GroupType {
    STRING,
    NUMERIC
  }

  // list of group, will merged into al final group
  private final List<Map<String, BitSet>> groups = new ArrayList<>();

  // the group results: <GroupName: all-matching-records>
  @Getter
  private Map<String, BitSet> mergedGroup = new HashMap<>();

  /**
   * Conduct groupBy according to current fields.
   *
   * @param bs          bitMap, true means the record in time range
   * @param groupFields filed ot be group by
   * @param metaChunk   current metaChunk
   * @param dataChunk   current metaChunk
   */
  public void groupBy(BitSet bs, List<String> groupFields, MetaChunkRS metaChunk,
                      ChunkRS dataChunk, GranularityType groupFieldsGranularity) {

    System.out.println("Init OLAP query timeRanges, matched records = "
        +
        bs.cardinality() + ", total size of BS = " + bs.size());

    if (groupFields == null) {
      this.mergedGroup.put("all", bs);
      return;
    }
    // group By multiple fields,
    for (String groupField : groupFields) {
      MetaFieldRS metaField = metaChunk.getMetaField(groupField);
      FieldRS dataField = dataChunk.getField(groupField);
      switch (dataField.getFieldType()) {
        case UserKey:
        case Action:
        case Segment:
          groupOnField(dataField, bs, metaField, GroupType.STRING);
          break;
        case Metric:
          groupOnField(dataField, bs, metaField, GroupType.NUMERIC);
          break;
        case ActionTime:
          groupOnField(dataField, bs, groupFieldsGranularity);
          break;
        default:
          throw new UnsupportedOperationException(
              "Un-supported field type: " + dataField.getFieldType());
      }
    }
    // merge all existing groups
    mergeGroups();
  }

  /**
   * Group by one column.
   *
   * @param field     grouped filed read form one dataChunk.
   * @param bs        bitSet, filtered by selector
   * @param metaField grouped filed read form one metaChunk.
   * @param type      group by type.
   */
  private void groupOnField(FieldRS field, BitSet bs, MetaFieldRS metaField, GroupType type) {
    long beg = System.currentTimeMillis();
    Map<String, BitSet> group = new HashMap<>();
    // globalID: bitMap, multiple rows may have same localID
    Map<Integer, BitSet> id2Bs = new HashMap<>();
    // get all local ids ( hash ), or value ( range )

    int i = 0;
    while (i < bs.size()) {
      // get the true position starting from i
      int nextPos = bs.nextSetBit(i);
      // if no true, skip checking.
      if (nextPos < 0) {
        break;
      }
      int id = field.getValueByIndex(nextPos).getInt();
      if (id2Bs.get(id) == null) {
        BitSet groupBs = new BitSet(bs.size()); // number of records
        groupBs.set(nextPos);
        id2Bs.put(id, groupBs);
      } else {
        // if multiple records have the same globalID
        // set the record position (nextPos) to be true
        id2Bs.get(id).set(nextPos);
      }
      // now checking from the true position
      i = nextPos + 1;
    }

    // convert key from galobal id to string
    for (Map.Entry<Integer, BitSet> entry : id2Bs.entrySet()) {
      group.put(globalId2Str(metaField, entry.getKey(), type), entry.getValue());
    }

    this.groups.add(group);
    long end = System.currentTimeMillis();
    //System.out.println("group elapsed: " + (end - beg));
  }

  /**
   * Group by one column.
   *
   * @param field       grouped filed read form one dataChunk.
   * @param bs          bitSet, filtered by timeRange and previous.
   * @param granularity when groupField is actionTime, group by granularity.
   */
  private void groupOnField(FieldRS field, BitSet bs, GranularityType granularity) {
    assert field.getFieldType() == FieldType.ActionTime;
    long beg = System.currentTimeMillis();

    // timeStr: bitMap, multiple rows may have same localID
    Map<String, BitSet> timeStr2Bs = new HashMap<>();
    // get all local ids ( hash ), or value ( range )

    int i = 0;
    while (i < bs.size()) {
      int nextPos = bs.nextSetBit(i);
      if (nextPos < 0) {
        break;
      }
      // convert data int to month str according to granularity
      int dataInt = field.getValueByIndex(nextPos).getInt();
      SecondIntConverter converter = new SecondIntConverter();
      String dataStr = converter.getString(dataInt);
      // convert to month based string
      String[] parts = dataStr.split("-");
      String monStr;
      if (granularity == GranularityType.YEAR) {
        monStr = parts[0];
      } else if (granularity == GranularityType.MONTH) {
        monStr = String.join("-", parts[0], parts[1]);
      } else {
        monStr = dataStr;
      }

      if (!timeStr2Bs.containsKey(monStr)) {
        BitSet groupBs = new BitSet(bs.size());
        groupBs.set(nextPos);
        timeStr2Bs.put(monStr, groupBs);
      } else {
        timeStr2Bs.get(monStr).set(nextPos);
      }
      i = nextPos + 1;
    }

    this.groups.add(timeStr2Bs);
    long end = System.currentTimeMillis();
    //System.out.println("group elapsed: " + (end - beg));
  }

  /**
   * Merge multiple groups into mergedGroup.
   */
  private void mergeGroups() {
    this.mergedGroup = this.groups.get(0);
    // for each group, merge the mergedGroup and the group
    for (int i = 1; i < this.groups.size(); i++) {
      Map<String, BitSet> nextGroup = this.groups.get(i);
      Map<String, BitSet> merged = new HashMap<>();
      for (Map.Entry<String, BitSet> entry : this.mergedGroup.entrySet()) {
        // merge with curGroup
        for (Map.Entry<String, BitSet> nextEntry : nextGroup.entrySet()) {
          String groupName = entry.getKey() + "|" + nextEntry.getKey();
          BitSet bs = (BitSet) entry.getValue().clone();
          bs.and(nextEntry.getValue());
          merged.put(groupName, bs);
        }
      }
      this.mergedGroup = merged;
    }
  }

  /**
   * Convert to string.
   *
   * @param metaField metaField
   * @param value     value
   * @param type      type
   * @return string got from local id
   */
  private String globalId2Str(MetaFieldRS metaField, int value, GroupType type) {
    switch (type) {
      case STRING: {
        // value is globalId => the filed => string
        return metaField.get(value).map(FieldValue::getString).orElse("");
      }
      case NUMERIC: {
        return String.valueOf(value);
      }
      default:
        throw new UnsupportedOperationException();
    }
  }
}
