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

package com.nus.cool.core.iceberg.aggregator;

import com.nus.cool.core.iceberg.result.AggregatorResult;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;

import java.util.BitSet;
import java.util.Map;

public class AverageAggregator implements Aggregator {

    @Override
    public void process(Map<String, BitSet> groups, FieldRS field,
                        Map<String, AggregatorResult> resultMap, MetaFieldRS metaFieldRS) {
        InputVector value = field.getValueVector();
        for(Map.Entry<String, BitSet> entry : groups.entrySet()) {
            if (resultMap.get(entry.getKey()) == null) {
                String groupName = entry.getKey();
                AggregatorResult aggregatorResult = new AggregatorResult();
                resultMap.put(groupName, aggregatorResult);
            }
            if (resultMap.get(entry.getKey()).getAverage() == null) {
                BitSet bs = entry.getValue();
                long sum = 0;
                if (resultMap.get(entry.getKey()).getSum() == null) {
                    for (int i = 0; i < bs.size(); i++) {
                        int nextPos = bs.nextSetBit(i);
                        if (nextPos < 0) {
                            break;
                        }
                        sum += value.get(nextPos);
                        i = nextPos;
                    }
                    resultMap.get(entry.getKey()).setSum(sum);
                } else {
                    sum = resultMap.get(entry.getKey()).getSum();
                }
                int count = bs.cardinality();
                if(resultMap.get(entry.getKey()).getCount() == null) {
                    resultMap.get(entry.getKey()).setCount(count);
                }
                float mean = sum / count;
                resultMap.get(entry.getKey()).setAverage(mean);
            }
        }
    }
}
