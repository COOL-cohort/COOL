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

package com.nus.cool.core.iceberg.result;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AggregatorResult {

    private Float count;

    private Long sum;

    private Float average;

    private Float max;

    private Float min;

    private Float countDistinct;

    @JsonIgnore
    private Set<String> distinctSet = new HashSet<>();

    public void merge(AggregatorResult res) {
        if (this.countDistinct != null) {
            this.distinctSet.addAll(res.getDistinctSet());
            this.countDistinct = (float) this.distinctSet.size();
        }
        if (this.max != null) this.max = this.max >= res.getMax() ? this.max : res.getMax();
        if (this.min != null) this.min = this.min <= res.getMin() ? this.min : res.getMin();
        if (this.count != null) this.count += res.getCount();
        if (this.sum != null) this.sum += res.getSum();
        if (this.average != null) this.average = this.sum / this.count;
     }
}
