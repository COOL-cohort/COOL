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

import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.converter.DayIntConverter;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.cohort.filter.FieldFilterFactory;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class IcebergSelection {

    private static Log LOG = LogFactory.getLog(IcebergSelection.class);

    private final FieldFilterFactory filterFactory = new FieldFilterFactory();

    private boolean bActivateCublet;

    private TableSchema tableSchema;

    private SelectionFilter filter;

    private IcebergQuery.granularityType granularity;

    private MetaChunkRS metaChunk;

    private String timeRange;

    private int max;

    private int min;

    private List<String> timeRanges;

    private List<Integer> maxs;

    private List<Integer> mins;

    private void splitTimeRange() throws ParseException {
        // TODO: format time range
        this.timeRanges = new ArrayList<>();
        this.maxs = new ArrayList<>();
        this.mins = new ArrayList<>();
        DayIntConverter converter = new DayIntConverter();
        switch(granularity) {
            case DAY:
                for (int i = 0; i < this.max - this.min; i++) {
                    int time = this.min + i;
                    this.maxs.add(time);
                    this.mins.add(time);
                    this.timeRanges.add(converter.getString(time));
                }
                break;
            case MONTH: {
                String[] timePoints = this.timeRange.split("\\|");
                Date d1 = new SimpleDateFormat("yyyy-MM").parse(timePoints[0]);
                Date d2 = new SimpleDateFormat("yyyy-MM").parse(timePoints[1]);
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(d2);
                calendar.add(Calendar.MONTH, 2);
                d2 = calendar.getTime();
                calendar.setTime(d1);
                List<String> points = new ArrayList<>();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                // add all data before end of range
                while (calendar.getTime().before(d2)) {
                    points.add(sdf.format(calendar.getTime()));
                    calendar.add(Calendar.MONTH, 1);
                }
                for (int i = 0; i < points.size() - 1; i++) {
                    this.timeRanges.add(points.get(i) + "|" + points.get(i + 1));
                    this.mins.add(converter.toInt(points.get(i)));
                    this.maxs.add(converter.toInt(points.get(i + 1)) - 1);
                }
                this.mins.set(0, this.min);
                this.maxs.set(this.maxs.size() - 1, this.max);

                break;
            }
            case YEAR: {
                String[] timePoints = this.timeRange.split("\\|");
                Date d1 = new SimpleDateFormat("yyyy").parse(timePoints[0]);
                Date d2 = new SimpleDateFormat("yyyy").parse(timePoints[1]);
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(d2);
                calendar.add(Calendar.YEAR, 2);
                d2 = calendar.getTime();
                calendar.setTime(d1);
                List<String> points = new ArrayList<>();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                while (calendar.getTime().before(d2)) {
                    points.add(sdf.format(calendar.getTime()));
                    calendar.add(Calendar.YEAR, 1);
                }
                for (int i = 0; i < points.size() - 1; i++) {
                    this.timeRanges.add(points.get(i) + "|" + points.get(i + 1));
                    this.mins.add(converter.toInt(points.get(i)));
                    this.maxs.add(converter.toInt(points.get(i + 1)));
                }
                break;
            }
            case NULL:
                this.maxs.add(this.max);
                this.mins.add(this.min);
                this.timeRanges.add(this.timeRange);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private void selectFields(BitSet bs, FieldRS field, FieldFilter filter) {
        InputVector fieldIn = field.getValueVector();
        //fieldIn.skipTo(beg);
        int off = 0;
        while (off < fieldIn.size() && off >= 0) {
            fieldIn.skipTo(off);
            if(!filter.accept(fieldIn.next())) {
                bs.clear(off);
            }
            off = bs.nextSetBit(off + 1);
        }
    }

    private SelectionFilter init(SelectionQuery selection) {
        if (selection == null) return null;
        SelectionFilter filter = new SelectionFilter();
        filter.setType(selection.getType());
        if (filter.getType().equals(SelectionQuery.SelectionType.filter)) {
            FieldFilter fieldFilter = FieldFilterFactory.create(
                    this.tableSchema.getFieldSchema(selection.getDimension()), null, selection.getValues());
            filter.setFilter(fieldFilter);
            filter.setDimension(selection.getDimension());
        } else {
            for (SelectionQuery childSelection : selection.getFields()) {
                SelectionFilter childFilter = init(childSelection);
                filter.getFields().add(childFilter);
            }
        }
        return filter;
    }

    private boolean process(SelectionFilter selectionFilter, MetaChunkRS metaChunk) {
        if (selectionFilter == null) return true;
        if (selectionFilter.getType().equals(SelectionQuery.SelectionType.filter)) {
            MetaFieldRS metaField = metaChunk.getMetaField(selectionFilter.getDimension());
            return selectionFilter.getFilter().accept(metaField);
        } else {
            boolean flag = selectionFilter.getType().equals(SelectionQuery.SelectionType.and);
            for (SelectionFilter childFilter : selectionFilter.getFields()) {
                if (selectionFilter.getType().equals(SelectionQuery.SelectionType.and)) {
                    flag &= process(childFilter, metaChunk);
                } else {
                    flag |= process(childFilter, metaChunk);
                }
            }
            return flag;
        }
    }

    private boolean process(SelectionFilter selectionFilter, ChunkRS chunk) {
        if (selectionFilter == null) return true;
        if (selectionFilter.getType().equals(SelectionQuery.SelectionType.filter)) {
            FieldRS field = chunk.getField(selectionFilter.getDimension());
            return selectionFilter.getFilter().accept(field);
        } else {
            boolean flag = selectionFilter.getType().equals(SelectionQuery.SelectionType.and);
            for (SelectionFilter childFilter : selectionFilter.getFields()) {
                if (selectionFilter.getType().equals(SelectionQuery.SelectionType.and)) {
                    flag &= process(childFilter, chunk);
                } else {
                    flag |= process(childFilter, chunk);
                }
            }
            return flag;
        }
    }

    private BitSet select(SelectionFilter selectionFilter, ChunkRS chunk, BitSet bv) {
        BitSet bs = (BitSet) bv.clone();
        if (selectionFilter == null) return bs;
        if (selectionFilter.getType().equals(SelectionQuery.SelectionType.filter)) {
            FieldRS field = chunk.getField(this.tableSchema.getFieldID(selectionFilter.getDimension()));
            InputVector keyVector = field.getKeyVector();
//            BitSet[] bitSets = field.getBitSets();
//            if (field.isPreCal()) {
//                List<String> values = selectionFilter.getFilter().getValues();
//                for (String value : values) {
//                    int gId = this.metaChunk.getMetaField(selectionFilter.getDimension()).find(value);
//                    int localId = keyVector.find(gId);
//                    bs.and(bitSets[localId]);
//                }
//            } else {
//                selectFields(bs, field, selectionFilter.getFilter());
//            }
            selectFields(bs, field, selectionFilter.getFilter());

        } else if (selectionFilter.getType().equals(SelectionQuery.SelectionType.and)) {
            for (SelectionFilter childFilter : selectionFilter.getFields()) {
                bs = select(childFilter, chunk, bs);
            }
        } else if (selectionFilter.getType().equals(SelectionQuery.SelectionType.or)) {
            List<BitSet> bitSets = new ArrayList<>();
            for (SelectionFilter childFilter : selectionFilter.getFields()) {
                bitSets.add(select(childFilter, chunk, bs));
            }
            bs = orBitSets(bitSets);
        }
        return bs;
    }

    private BitSet orBitSets(List<BitSet> bitSets) {
        BitSet bs = bitSets.get(0);
        for (int i = 1; i < bitSets.size(); i++) {
            bs.or(bitSets.get(i));
        }
        return bs;
    }

    private boolean accept(int min, int max) {
        if (this.timeRange == null) return true;
        return (min <= this.max) && (max >= this.min);
    }

    public void init(TableSchema tableSchema, IcebergQuery query) throws ParseException{
        this.tableSchema = checkNotNull(tableSchema);
        query = checkNotNull(query);

        SelectionQuery selection = query.getSelection();
        this.filter = init(selection);

        if (query.getTimeRange() != null) {
            this.timeRange = query.getTimeRange();
            this.granularity = query.getGranularity();
            String[] timePoints = this.timeRange.split("\\|");
            DayIntConverter converter = new DayIntConverter();
            this.min = converter.toInt(timePoints[0]);
            this.max = converter.toInt(timePoints[1]);
            splitTimeRange();
        }
    }

    // process metaChunk
    public void process(MetaChunkRS metaChunk) {
        this.metaChunk = metaChunk;
        int actionField = this.tableSchema.getActionTimeField();
        MetaFieldRS timeField = metaChunk.getMetaField(actionField, FieldType.ActionTime);
        // min and max must be in valid range
        boolean isAccepted = accept(timeField.getMinValue(), timeField.getMaxValue());
        // must have and
        boolean isProcess = process(this.filter, metaChunk);

        // if both accepted, then it's activate
        this.bActivateCublet = isAccepted  && isProcess;
    }

    // process dataChunk
    public Map<String, BitSet> process(ChunkRS chunk) {
        FieldRS timeField = chunk.getField(this.tableSchema.getActionTimeField());
        int minKey = timeField.minKey();
        int maxKey = timeField.maxKey();
        if (!(accept(minKey, maxKey) && process(this.filter, chunk))) {
            return null;
        }
        Map<String, BitSet> map = new HashMap<>();
        if (this.timeRange == null) {
            BitSet bv = new BitSet(chunk.records());
            bv.set(0, chunk.records());
            map.put("no time filter", bv);
        } else {
            long beg = System.currentTimeMillis();
            int tag = 0;
            while (minKey > this.maxs.get(tag)) tag += 1;
            if (minKey >= this.mins.get(tag) & maxKey <= this.maxs.get(tag)) {
                BitSet bv = new BitSet(chunk.records());
                bv.set(0, bv.size());
                map.put(this.timeRanges.get(tag), bv);
            } else {
                InputVector timeInput = timeField.getValueVector();
                timeInput.skipTo(0);
                BitSet[] bitSets = new BitSet[this.timeRanges.size()];
                for (int i = 0; i < this.timeRanges.size(); i++) {
                    bitSets[i] = new BitSet(chunk.records());
                }
                int min = this.mins.get(tag);
                int max = this.maxs.get(tag);
                for (int i = 0; i < timeInput.size(); i++) {
                    int time = timeInput.next();
                    if (time < this.min) continue;
                    if (time > this.max) break;
                    if (time >= min && time <= max) {
                        bitSets[tag].set(i);
                    } else {
                        while (!(time >= this.mins.get(tag) && (time <= this.maxs.get(tag)))) tag += 1;
                        min = this.mins.get(tag);
                        max = this.maxs.get(tag);
                        bitSets[tag].set(i);
                    }
                }
                for (int i = 0; i < bitSets.length; i++) {
                    if (bitSets[i].cardinality() != 0) {
                        map.put(this.timeRanges.get(i), bitSets[i]);
                    }
                }
            }
            long end = System.currentTimeMillis();
            //System.out.println("process time filter elapsed: " + (end - beg));
        }
        for (Map.Entry<String, BitSet> entry : map.entrySet()) {
            BitSet bs = select(this.filter, chunk, entry.getValue());
            map.put(entry.getKey(), bs);
        }
        return map;
    }

    public boolean isbActivateCublet() {
        return this.bActivateCublet;
    }

}
