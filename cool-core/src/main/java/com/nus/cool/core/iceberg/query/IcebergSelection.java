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

    private boolean bActivateCublet;

    private TableSchema tableSchema;

    private SelectionFilter filter;

    private IcebergQuery.granularityType granularity;

    private String timeRange;

    // max and min query Time specified by user
    private int maxQueryTime;
    private int minQueryTime;

    // time range split by system, used as key in map
    private List<String> timeRanges;

    // store multiple time range, convert data into integer
    private List<Integer> maxTimeRanges;
    private List<Integer> minTimeRanges;

    /**
     * Split time range according to user specified granularity
     * @throws ParseException ParseException
     */
    private void splitTimeRange() throws ParseException {
        // TODO: format time range
        this.timeRanges = new ArrayList<>();
        this.maxTimeRanges = new ArrayList<>();
        this.minTimeRanges = new ArrayList<>();
        DayIntConverter converter = new DayIntConverter();

        switch(granularity) {
            case DAY:
                for (int i = 0; i < this.maxQueryTime - this.minQueryTime; i++) {
                    int time = this.minQueryTime + i;
                    this.maxTimeRanges.add(time);
                    this.minTimeRanges.add(time);
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
                    this.minTimeRanges.add(converter.toInt(points.get(i)));
                    this.maxTimeRanges.add(converter.toInt(points.get(i + 1)) - 1);
                }
                this.minTimeRanges.set(0, this.minQueryTime);
                this.maxTimeRanges.set(this.maxTimeRanges.size() - 1, this.maxQueryTime);

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
                    this.minTimeRanges.add(converter.toInt(points.get(i)));
                    this.maxTimeRanges.add(converter.toInt(points.get(i + 1)));
                }
                break;
            }
            case NULL:
                this.maxTimeRanges.add(this.maxQueryTime);
                this.minTimeRanges.add(this.minQueryTime);
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

    /**
     * Init Filter from origin selection
     * @param selection selection of query file.
     * @return filter instance, parsed after checking selection type. (only supports and, or, filter)
     */
    private SelectionFilter init(SelectionQuery selection) {
        if (selection == null) return null;
        SelectionFilter filter = new SelectionFilter();
        filter.setType(selection.getType());
        // filter requires 'dimension', 'values'
        if (filter.getType().equals(SelectionQuery.SelectionType.filter)) {
            FieldFilter fieldFilter = FieldFilterFactory.create(
                    this.tableSchema.getFieldSchema(selection.getDimension()), null, selection.getValues());
            filter.setFilter(fieldFilter);
            filter.setDimension(selection.getDimension());
        }
        // and , or requires 'fields' .
        else {
            for (SelectionQuery childSelection : selection.getFields()) {
                SelectionFilter childFilter = init(childSelection);
                filter.getFields().add(childFilter);
            }
        }
        return filter;
    }

    /**
     * Check if current cublet requires seleciton,
     * @param selectionFilter parsed from query.json
     * @param metaChunk the checked metaChunk
     * @return true: this cublet meet the requirements of seleciton, false: skip processing this.
     */
    private boolean process(SelectionFilter selectionFilter, MetaChunkRS metaChunk) {
        if (selectionFilter == null) return true;
        // if this is filter, then
        if (selectionFilter.getType().equals(SelectionQuery.SelectionType.filter)) {
            MetaFieldRS metaField = metaChunk.getMetaField(selectionFilter.getDimension());
            return selectionFilter.getFilter().accept(metaField);
        } else {
            // recursively check if this cublet requires checking
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
        return (min <= this.maxQueryTime) && (max >= this.minQueryTime);
    }

    public void init(TableSchema tableSchema, IcebergQuery query) throws ParseException{
        this.tableSchema = checkNotNull(tableSchema);
        checkNotNull(query);

        // parse selection from query.json
        SelectionQuery selection = query.getSelection();
        this.filter = init(selection);

        // parse time range from query.json
        if (query.getTimeRange() != null) {
            this.timeRange = query.getTimeRange();
            this.granularity = query.getGranularity();
            String[] timePoints = this.timeRange.split("\\|");
            DayIntConverter converter = new DayIntConverter();
            this.minQueryTime = converter.toInt(timePoints[0]);
            this.maxQueryTime = converter.toInt(timePoints[1]);
            splitTimeRange();
        }
    }

    // process metaChunk
    public void process(MetaChunkRS metaChunk) {
        int actionFieldIndex = this.tableSchema.getActionTimeField();
        MetaFieldRS timeField = metaChunk.getMetaField(actionFieldIndex, FieldType.ActionTime);
        // min and max must be in valid range
        boolean isAccepted = accept(timeField.getMinValue(), timeField.getMaxValue());
        // must have
        boolean isProcess = process(this.filter, metaChunk);

        // if both accepted, then it's activate
        this.bActivateCublet = isAccepted  && isProcess;
    }

    /**
     * process dataChunk filter with time range
     * @param chunk: dataChunk
     * @return map of < time_range : bitMap >,
     *  where each element in biteMap is one record,
     *              true = the record meet the requirement,
     *              false = the record don't meet the requirement.
     */
    public Map<String, BitSet> process(ChunkRS chunk) {
        FieldRS timeField = chunk.getField(this.tableSchema.getActionTimeField());
        // get the min max time of this dataChunk
        int minKey = timeField.minKey();
        int maxKey = timeField.maxKey();
        if (!(accept(minKey, maxKey) && process(this.filter, chunk))) {
            return null;
        }

        Map<String, BitSet> map = new HashMap<>();
        // if the query don't provide timeRange, all record is true
        if (this.timeRange == null) {
            BitSet bv = new BitSet(chunk.records());
            bv.set(0, chunk.records());
            map.put("no time filter", bv);
        }
        // if the query don't provide the timeRange
        else {
            long beg = System.currentTimeMillis();
            // time min-max combination id
            int tag = 0;
            // find min-max combination where min is under this range
            while (minKey > this.maxTimeRanges.get(tag)) tag += 1;

            // if all data in this dataChunk satisfied the current range at key=tage
            if (minKey >= this.minTimeRanges.get(tag) & maxKey <= this.maxTimeRanges.get(tag)) {
                BitSet bv = new BitSet(chunk.records());
                bv.set(0, bv.size());
                map.put(this.timeRanges.get(tag), bv);
            }
            // traverse each record from beginning
            else {
                InputVector timeInput = timeField.getValueVector();
                timeInput.skipTo(0);
                BitSet[] bitSets = new BitSet[this.timeRanges.size()];
                for (int i = 0; i < this.timeRanges.size(); i++) {
                    bitSets[i] = new BitSet(chunk.records());
                }
                int min = this.minTimeRanges.get(tag);
                int max = this.maxTimeRanges.get(tag);
                for (int i = 0; i < timeInput.size(); i++) {
                    int time = timeInput.next();
                    if (time < this.minQueryTime) continue; // this record is not in the range
                    if (time > this.maxQueryTime) break; // the rest all greater than this.max. not in range, skip
                    if (time >= min && time <= max) {
                        bitSets[tag].set(i);
                    } else {
                        while (!(time >= this.minTimeRanges.get(tag) && (time <= this.maxTimeRanges.get(tag)))) tag += 1;
                        min = this.minTimeRanges.get(tag);
                        max = this.maxTimeRanges.get(tag);
                        bitSets[tag].set(i);
                    }
                }

                for (int i = 0; i < bitSets.length; i++) {
                    // if the number of true in bitMap > 0
                    if (bitSets[i].cardinality() != 0) {
                        map.put(this.timeRanges.get(i), bitSets[i]); //  record the corresponding records.
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
