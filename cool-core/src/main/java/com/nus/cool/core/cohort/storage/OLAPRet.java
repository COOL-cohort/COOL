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

package com.nus.cool.core.cohort.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nus.cool.core.cohort.aggregate.AggregateType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Base result class.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OLAPRet {

  @Data
  private static class AggregatorResult {

    private Integer count;

    private Long sum;

    private Float average;

    private Integer max;

    private Integer min;

    private Float countDistinct;

    @JsonIgnore
    private Set<String> distinctSet = new HashSet<>();

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if ((o instanceof AggregatorResult)) {
        AggregatorResult another = (AggregatorResult) o;

        if (!another.toString().equals(this.toString())) {
          return false;
        }
        return true;
      } else {
        return false;
      }
    }

    public String toString() {

      String json = "\n{";

      if (this.count != null) {
        json += "\n  count:" + count.toString();
      }
      if (this.sum != null) {
        json += "\n  count:" + sum.toString();
      }
      if (this.average != null) {
        json += "\n  count:" + average.toString();
      }
      if (this.max != null) {
        json += "\n  count:" + max.toString();
      }
      if (this.min != null) {
        json += "\n  count:" + min.toString();
      }
      if (this.countDistinct != null) {
        json += "\n  count:" + countDistinct.toString();
      }

      json += "}";

      return json;
    }

    /**
     * Merge aggregation results.
     */
    public void merge(AggregatorResult res) {
      if (this.countDistinct != null) {
        this.distinctSet.addAll(res.getDistinctSet());
        this.countDistinct = (float) this.distinctSet.size();
      }
      if (this.max != null) {
        this.max = this.max >= res.getMax() ? this.max : res.getMax();
      }
      if (this.min != null) {
        this.min = this.min <= res.getMin() ? this.min : res.getMin();
      }
      if (this.count != null) {
        this.count += res.getCount();
      }
      if (this.sum != null) {
        this.sum += res.getSum();
      }
      if (this.average != null) {
        this.average = this.sum / (float) this.count;
      }
    }
  }

  private String timeRange;

  private String key;

  private String fieldName;

  private AggregatorResult aggregatorResult = new AggregatorResult();

  private boolean equalsKey(OLAPRet another) {
    Set<String> set1 = new HashSet<>(Arrays.asList(this.getKey().split("|")));
    Set<String> set2 = new HashSet<>(Arrays.asList(another.getKey().split("|")));
    return set1.equals(set2);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if ((o instanceof OLAPRet)) {
      OLAPRet another = (OLAPRet) o;
      if (this.equalsKey(another)) {

        if (!this.getFieldName().equals((another.getFieldName()))) {
          return false;
        }

        if (!this.getAggregatorResult().equals((another.getAggregatorResult()))) {
          return false;
        }

        return true;
      } else {
        // not same key
        return false;
      }
    } else {
      // not OLAPRet instance
      return false;
    }
  }

  /**
   * initAggregator.
   *
   * @param res res.
   */
  public void initAggregator(Map<AggregateType, RetUnit> res) {

    for (Map.Entry<AggregateType, RetUnit> entry : res.entrySet()) {
      switch (entry.getKey()) {
        case SUM:
          this.aggregatorResult.setSum((long) entry.getValue().getValue());
          continue;
        case AVERAGE:
          this.aggregatorResult.setAverage(entry.getValue().getValue());
          continue;
        case MAX:
          this.aggregatorResult.setMax((int) entry.getValue().getValue());
          continue;
        case MIN:
          this.aggregatorResult.setMin((int) entry.getValue().getValue());
          continue;
        case COUNT:
          this.aggregatorResult.setCount((int) entry.getValue().getValue());
          continue;
        case DISTINCT:
          this.aggregatorResult.setCountDistinct(entry.getValue().getValue());
          continue;
        default:
          throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * Merge two base results.
   */
  public static List<OLAPRet> merge(List<OLAPRet> results) {
    BitSet bs = new BitSet();
    bs.set(0, results.size());
    for (int i = 0; i < bs.size(); i++) {
      i = bs.nextSetBit(i);
      if (i < 0) {
        break;
      }
      OLAPRet res1 = results.get(i);
      for (int j = i + 1; j < bs.size(); j++) {
        j = bs.nextSetBit(j);
        if (j < 0) {
          break;
        }
        OLAPRet res2 = results.get(j);
        if (res1.equals(res2)) {
          res1.getAggregatorResult().merge(res2.getAggregatorResult());
          bs.clear(j);
        }
      }
    }
    List<OLAPRet> finalRes = new ArrayList<>();
    for (int i = 0; i < bs.size(); i++) {
      i = bs.nextSetBit(i);
      if (i < 0) {
        break;
      }
      finalRes.add(results.get(i));
    }
    // System.out.println("bs: " + finalRes.size());
    return finalRes;
  }

  /**
   * to string.
   *
   * @return string.
   */
  public String toString() {

    String json = "\n{timeRange: " + timeRange + "\nkey:, " + key + "\nfieldName:" + fieldName
        +
        "\naggregatorResult:" + aggregatorResult.toString();
    return json;
  }

}
