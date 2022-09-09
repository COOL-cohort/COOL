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
package com.nus.cool.core.cohort;

import com.nus.cool.core.cohort.aggregator.EventAggregator;
import com.nus.cool.core.cohort.filter.AgeFieldFilter;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.cohort.filter.FieldFilterFactory;
import com.nus.cool.core.cohort.filter.SetFieldFilter;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.DataType;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ExtendedCohortSelection implements Operator {

	private final FieldFilterFactory filterFactory = new FieldFilterFactory();

	private final List<Map<String, FieldFilter>> birthFilters = new ArrayList<>();

	private final List<Map<String, FieldFilter>> birthAggFilters = new ArrayList<>();

	// Record the minimal time frequencies of birth events
	private int[] minTriggerTime;

	// Record the maximal time frequencies of birth events
	private int[] maxTriggerTime;

	private final Map<String, FieldFilter> ageFilters = new HashMap<>();

	private final Map<String, FieldFilter> ageByFilters = new HashMap<>();

	private TableSchema tableSchema;

	private InputVector timeVector;

	private int maxDate;

	private boolean bUserActiveCublet;

	private boolean bUserActiveChunk;

	private boolean bAgeActiveChunk;

	private FieldFilter ageFilter;

	private ExtendedCohortQuery query;

	private final ArrayList<LinkedList<Integer>> eventOffset = new ArrayList<>();

	private final ExtendedCohort cohort = new ExtendedCohort();

	private ChunkRS chunk;


	public void init(TableSchema tableSchema, ExtendedCohortQuery q) {
		this.tableSchema = checkNotNull(tableSchema);
		this.query = checkNotNull(q);

		// process birth selector
		BirthSequence seq = q.getBirthSequence();
		this.minTriggerTime = new int[seq.getBirthEvents().size()];
		this.maxTriggerTime = new int[seq.getBirthEvents().size()];

		int idx = 0;
		for (BirthSequence.BirthEvent e : seq.getBirthEvents()) {
			Map<String, FieldFilter> filters = new HashMap<>();
			Map<String, FieldFilter> aggrFilters = new HashMap<>();

			// handle birth selection filters
			for (ExtendedFieldSet fs : e.getEventSelection()) {
				String fn = fs.getCubeField();
				FieldSchema schema = tableSchema.getField(fn);
				filters.put(fn, FieldFilterFactory.create(schema, fs, fs.getFieldValue().getValues()));
			}

			// handle aggregate selection filters
			for (ExtendedFieldSet fs : e.getAggrSelection()) {
				String fn = fs.getCubeField();
				FieldSchema schema = tableSchema.getField(fn);
				aggrFilters.put(fn, FieldFilterFactory.create(schema, fs, fs.getFieldValue().getValues()));
			}

			this.birthFilters.add(filters);
			this.birthAggFilters.add(aggrFilters);

			// this.birthFilterFields.add(field);
			minTriggerTime[idx] = e.getMinTrigger();
			maxTriggerTime[idx] = e.getMaxTrigger();

			this.eventOffset.add(new LinkedList<Integer>());
			++idx;
		}

		// Process age-by and age selectors
		if (q.getAgeField() == null) return;

		List<ExtendedFieldSet> selectors;
		Map<String, FieldFilter> filterMap;

		// process ageField
		int fieldID = tableSchema.getFieldID(q.getAgeField().getField());
		checkArgument(fieldID >= 0);
		selectors = q.getAgeField().getEventSelection();
		List<String> ageRange = q.getAgeField().getRange();
		if (ageRange == null || ageRange.isEmpty()) {
			//hardcode an AgeFieldFilter
			ageRange = new ArrayList<>();
			ageRange.add(String.valueOf(Integer.MIN_VALUE)
					+ "|" + String.valueOf(Integer.MAX_VALUE));
		}
		this.ageFilter = new AgeFieldFilter(ageRange);

		filterMap = this.ageByFilters;
		if (selectors != null) {
			for (ExtendedFieldSet fs : selectors) {
				String field = fs.getCubeField();
				filterMap.put(field,
						FieldFilterFactory.create(tableSchema.getField(field), fs, fs.getFieldValue().getValues()));
			}
		}

		// process ageSelection
		selectors = q.getAgeSelection();
		filterMap = this.ageFilters;
		if (selectors != null) {
			for (ExtendedFieldSet fs : selectors) {
				String field = fs.getCubeField();
				filterMap.put(field,
						FieldFilterFactory.create(tableSchema.getField(field), fs, fs.getFieldValue().getValues()));
			}
		}
	}

	@Override
	public void init(TableSchema schema, CohortQuery query) {

	}

	@Override
	public void process(MetaChunkRS metaChunk) {
		bUserActiveCublet = true;

		// Process birth filter
		int idx = 0;
		for (Map<String, FieldFilter> birthFilter : birthFilters) {
			for (Map.Entry<String, FieldFilter> entry : birthFilter.entrySet()) {
				MetaFieldRS metaField = metaChunk.getMetaField(entry.getKey());
				if (minTriggerTime[idx] > 0)
					bUserActiveCublet &= entry.getValue().accept(checkNotNull(metaField));
				else
					entry.getValue().accept(metaField);
			}
			idx++;
		}

		// process birth aggregation filter
		for (Map<String, FieldFilter> birthFilter : this.birthAggFilters) {
			for (Map.Entry<String, FieldFilter> entry : birthFilter.entrySet()) {
				bUserActiveCublet &= entry.getValue().accept(metaChunk.getMetaField(entry.getKey()));
			}
 		}

		// Process age by and age filter
		for (Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
			MetaFieldRS metaField = metaChunk.getMetaField(entry.getKey());
			entry.getValue().accept(checkNotNull(metaField));
		}

		// age by event/dimension
		for (Map.Entry<String, FieldFilter> entry : ageByFilters.entrySet()) {
			MetaFieldRS metaField = metaChunk.getMetaField(entry.getKey());
			entry.getValue().accept(checkNotNull(metaField));
		}

		this.maxDate = TimeUtils.getDate(
				metaChunk.getMetaField(tableSchema.getActionTimeField(), FieldType.ActionTime).getMaxValue());
	}

	@Override
	public void process(ChunkRS chunk) {

		bUserActiveChunk = true;
		bAgeActiveChunk = true;

		this.chunk = chunk;

		for (int i = 0; i < birthFilters.size(); i++) {
			Map<String, FieldFilter> filterMap = birthFilters.get(i);

			for (Map.Entry<String, FieldFilter> entry : filterMap.entrySet()) {
				FieldRS field = chunk.getField(entry.getKey());
				if (minTriggerTime[i] > 0)
					bUserActiveChunk &= entry.getValue().accept(field);
				else
					// todo: why do we need to run accept if this filed is not considered
					entry.getValue().accept(field);
			}
		}

		// Process birth aggregation filter
		for (Map<String, FieldFilter> birthFilter : this.birthAggFilters) {
			for (Map.Entry<String, FieldFilter> entry : birthFilter.entrySet()) {
				bUserActiveChunk &= entry.getValue().accept(chunk.getField(entry.getKey()));
			}
		}

		// Process ageSelection filter
		for (Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
			FieldRS field = chunk.getField(entry.getKey());
			bAgeActiveChunk &= entry.getValue().accept(field);
			// ageFilterFields.put(entry.getKey(), field);
		}

		// Process ageField filter
		for (Map.Entry<String, FieldFilter> entry : ageByFilters.entrySet()) {
			FieldRS field = chunk.getField(entry.getKey());
			bAgeActiveChunk &= entry.getValue().accept(field);
			// ageByFilterFields.put(entry.getKey(), field);
		}

		timeVector = chunk.getField(tableSchema.getActionTimeField()).getValueVector();
	}

	/**
	 * @brief find the next tuple where the given @param event is qualified with
	 *        respect to the birth filter
	 *
	 * @param event
	 * @param fromOffset
	 * @param endOffset
	 *
	 * @return the offset of the qualified tuple, or @param endOffset if not
	 *         found
	 */
	private int skipToNextQualifiedBirthTuple(int event, int fromOffset, int endOffset) {

		int passed = 0;
		int nextOffset;
		Collection<FieldFilter> filterCollection = birthFilters.get(event).values();
		int numFilter = filterCollection.size();
		if (fromOffset >= endOffset || numFilter == 0)
			return fromOffset;

		// The while-loop iteratively finds the next common offset.
		// Termination condition:
		// If a common offset is found, i.e., passed == numFilter, then nextOffset (< endOffset) will be returned.
		// If no common offset can be found, i.e., fromOffset >= endOffset, then nextOffset (>= endOffset) will be returned.
		while (true) {
			for (FieldFilter currentFilter : filterCollection) {
				nextOffset = currentFilter.nextAcceptTuple(fromOffset, endOffset);
				// TODO check this logic
				if (nextOffset > fromOffset) {
					fromOffset = nextOffset;
					passed = 1;
				} else { // i.e., nextOffset == fromOffset
					passed++;
				}
				if (passed == numFilter || fromOffset >= endOffset)
					return fromOffset;
			}
		}
	}

	/**
	 * @brief check if the number of occurrence of each birth event follows the
	 *        max/min trigger condition or not
	 *
	 * @return true if follows; false otherwise
	 */
	private boolean checkOccurrence(int e) {
		int occurrence = this.eventOffset.get(e).size();
		if (minTriggerTime[e] > occurrence || (maxTriggerTime[e] >= 0 && maxTriggerTime[e] < occurrence))
			return false;
		return true;
	}

	/**
	 * @brief check for each birth event if its aggregated value passes the
	 *        aggregation filter or not
	 *
	 * @return true if passes; false otherwise
	 */
	private boolean filterByBirthAggregation() {
		boolean r = true;
		FieldFilter filter;
		Map<String, FieldFilter> filters;
		for (int i = 0; r && i < birthAggFilters.size(); i++) {
			filters = this.birthAggFilters.get(i);

			for (Map.Entry<String, FieldFilter> entry : filters.entrySet()) {
				Double birthAttr = getBirthAttribute(i, tableSchema.getFieldID(entry.getKey()));

				if (birthAttr == null)
					return false; // aggregation value does not exist

				if (entry.getValue() instanceof SetFieldFilter) {
					r = r && entry.getValue().accept(birthAttr.intValue());
				} else {
					filter = entry.getValue();
					ExtendedFieldSet.FieldValue value = filter.getFieldSet().getFieldValue();
					if (value.getType() != ExtendedFieldSet.FieldValueType.AbsoluteValue) {
						filter.updateValues(
								getBirthAttribute(value.getBaseEvent(), tableSchema.getFieldID(value.getBaseField())));
					}
					r = r && filter.accept(birthAttr.intValue());
				}

				if (!r)
					return r;
			}
		}
		return r;
	}

	/**
	 * @brief evaluate the birth filters against tuples within the given offset
	 *        range
	 * @param eventId
	 *            the id of birth event
	 * @param fromOffset
	 *            the starting point
	 * @param endOffset
	 */
	private void filterEvent(int eventId, int fromOffset, int endOffset) {
		LinkedList<Integer> offset = eventOffset.get(eventId);
		while (fromOffset < endOffset) {
			fromOffset = skipToNextQualifiedBirthTuple(eventId, fromOffset, endOffset);
			if (fromOffset < endOffset) {
				offset.addLast(fromOffset);
				fromOffset++;
			}
		}
	}

	/**
	 *
	 * @param start start offset
	 * @param end end offset
	 * @return
	 */
	private int getUserBirthTime(int start, int end) {

		int offset = start;
		int birthOffset = start;
		int firstDay = timeVector.get(start);
		int birthDay = firstDay;

		BirthSequence seq = query.getBirthSequence();
		List<Integer> sortedEvents = seq.getSortedBirthEvents();
		LinkedList<Integer> occurrences;

		// starting date of each event time window, which is initialized as 0.
		int[] windowtDate = new int[sortedEvents.size()];

		for (Integer e : sortedEvents) {
			offset = start;
			BirthSequence.BirthEvent event = seq.getBirthEvents().get(e);
			// check whether the filed value type is AbsoluteValue
			for (FieldFilter ageFilter : birthFilters.get(e).values()) {
				ExtendedFieldSet.FieldValue value = ageFilter.getFieldSet().getFieldValue();
				if (value.getType() != ExtendedFieldSet.FieldValueType.AbsoluteValue) {
					ageFilter.updateValues(
							getBirthAttribute(value.getBaseEvent(), tableSchema.getFieldID(value.getBaseField())));
				}
			}

			// check time window
			BirthSequence.TimeWindow window = event.getTimeWindow();
			if (window == null) {
				// no time window
				// check the minimal trigger time
				for (int i = 0; i < minTriggerTime[e]; i++) {
					offset = skipToNextQualifiedBirthTuple(e, offset, end);
					if (offset >= end)
						return -1;
					eventOffset.get(e).addLast(offset);
					offset++;
				}
				int bday;
				if (minTriggerTime[e] == 0)
					bday = timeVector.get(offset);
				else
					bday = timeVector.get(offset - 1) + 1;

				//birthDay = (birthDay < bday) ? bday : birthDay;
				if (birthDay < bday) birthDay = bday ;

				// check the maximal trigger time
				if(maxTriggerTime[e]!=-1){
					int count = minTriggerTime[e];
					while(offset<end){
						offset = skipToNextQualifiedBirthTuple(e, offset, end);
						if(offset < end){
							count += 1;
							eventOffset.get(e).addLast(offset);
							offset++;
							if(count > maxTriggerTime[e]){
								return -1;
							}
						}
					}
				}

			} else {
				// with time window
				int startDay = firstDay;
				int wlen = window.getLength();
				// int endDay = maxDate - (wlen - 1); // at least one time window
				int endDay = TimeUtils.getDateFromOffset(timeVector,end-1);
				// remove users without records for at least wlen day
				if ((endDay-startDay)<wlen) return -1;

				int day;
				for (BirthSequence.Anchor anc : window.getAnchors()) {
					day = anc.getLowOffset() + windowtDate[anc.getAnchor()];
					//startDay = startDay < day ? day : startDay;
					if (startDay < day) startDay = day;
					day = anc.getHighOffset() + windowtDate[anc.getAnchor()];
					//endDay = endDay > day ? day : endDay;
					if (endDay > day) endDay = day;
				}

				if (wlen == 0) {
					// set the wlen to the maximum day if it is set to 0
					wlen = maxDate - startDay + 1;
					//endDay = (endDay >= startDay) ? startDay : endDay;
					if (endDay > startDay) endDay = startDay;
				}

				// offset records the checked time space
				offset = TimeUtils.skipToDate(timeVector, start, end, startDay);
				// windowOffset records the start time offset of a certain time window
				int windowOffset = offset;
				int pos;
				while (startDay <= endDay) {
					// skip to the endOffset for the time window
					pos = TimeUtils.skipToDate(timeVector, offset, end, startDay + wlen);

					// skip to the startOffset for the time window
					windowOffset = TimeUtils.skipToDate(timeVector, windowOffset, end, startDay);

					// delete the founded offsets that are ahead of the time window
					occurrences = eventOffset.get(e);
					while (!occurrences.isEmpty()) {
						if (occurrences.getFirst() >= windowOffset)
							break;
						occurrences.removeFirst();
					}

					// evaluate birth filters between the time range
					filterEvent(e, offset, pos);
					offset = pos;

					if (checkOccurrence(e)) {
						windowtDate[e] = startDay;
						//birthDay = (birthDay < startDay + wlen) ? startDay + wlen : birthDay;
						birthDay = Math.max(startDay + wlen, birthDay);
						break;
					}

					// not a slice window and occurrence check fails
					// if false, then we only check once
					if (!window.getSlice()) {
						return -1;
					}

					startDay++;
				}

				if (startDay > endDay)
					return -1;
			}

			// birthOffset points to the first age tuple
			//birthOffset = (birthOffset < offset) ? offset : birthOffset;
			if (birthOffset < offset) birthOffset = offset;
		}

		// evaluate birth aggregation filters
		if (filterByBirthAggregation()) {
			// real birthday
			cohort.setBirthDate(birthDay - 1);
			cohort.setBirthOffset(birthOffset);
			return birthOffset;
		}

		else
			return -1;
	}
	
	public ExtendedCohort selectUser(int start, int end) {
		checkArgument(start < end);

		// clean event offset
		for (LinkedList<Integer> occurrence : this.eventOffset) {
			occurrence.clear();
		}
		int boff = this.getUserBirthTime(start, end);
		cohort.clearDimension();
		if (boff >= 0) {
			// find the respective cohort for this user
			List<BirthSequence.BirthEvent> events = query.getBirthSequence().getBirthEvents();
			for (int idx = 0; idx < events.size(); ++idx) {
				BirthSequence.BirthEvent be = events.get(idx);
				for (BirthSequence.CohortField cf : be.getCohortFields()) {
					int fieldID = tableSchema.getFieldID(cf.getField());                    

					// cohort by the birth time
					// However, this code block seems not to be triggered during the test.
					if (fieldID == tableSchema.getActionTimeField()) {
						cohort.addDimension(TimeUtils.getDateofNextTimeUnitN(cohort.getBirthDate(),
								query.getAgeField().getUnit(), 0));
						continue;
					}

					Double value = getBirthAttribute(idx, fieldID);
					if (value == null)
						return null;

					// determine the dimension
					FieldSchema schema = tableSchema.getField(fieldID);
					if (schema.getDataType() == DataType.String) {
						// use the global id as the cohort dimension
						int gid = chunk.getField(cf.getField()).getKeyVector().get(value.intValue());
						cohort.addDimension(gid);
					} else {
						int level = cf.getMinLevel();
						Double v;
						if (cf.isLogScale()) {
							while (true) {
								v = Math.pow(cf.getScale(), level);
								if (value <= v || level >= cf.getNumLevel() + cf.getMinLevel()) {
									break;
								}
								++level;
							}
						} else {
							v = (value / cf.getScale());
							//level = (v > v.intValue()) ? v.intValue()+1 : v.intValue();
							//level = (level >= cf.getMinLevel()) ? level : cf.getMinLevel();
							//if (level >= cf.getMinLevel() + cf.getNumLevel())
								//level = cf.getMinLevel() + cf.getNumLevel();
							int v_int = v.intValue();
							level = Math.max(((v > v_int) ? v_int+1 : v_int), cf.getMinLevel());
							level = Math.min(level, cf.getMinLevel() + cf.getNumLevel());

						}
						cohort.addDimension(level);
					}
				}
			}
			return cohort;
		}
		return null;
	}
	

	private void filterAgeActivity(int ageOff, int ageEnd, BitSet bs, InputVector fieldIn, FieldFilter ageFilter) {
		// update value for this column if necessary
//		ExtendedFieldSet.FieldValue value = ageFilter.getFieldSet().getFieldValue();
//		if (value.getType() != FieldValueType.AbsoluteValue) {
//			ageFilter.updateValues(
//					getBirthAttribute(value.getBaseEvent(), tableSchema.getFieldID(value.getBaseField())));
//		}

		fieldIn.skipTo(ageOff);
		if (bs.cardinality() >= ((ageEnd - ageOff) >> 1)) {
			for (int i = ageOff; i < ageEnd; i++) {
				// int val = fieldIn.next();
				// if (!ageFilter.accept(val)) {
				if (!ageFilter.accept(fieldIn.next())) {
					bs.clear(i);
				}
			}
		} else {            
			int pos = bs.nextSetBit(ageOff);
			while (pos < ageEnd && pos >= 0) {
				// int val = fieldIn.get(pos);
				// if (!ageFilter.accept(val)) {
				if (!ageFilter.accept(fieldIn.get(pos))) {
					bs.clear(pos);
				}
				pos = bs.nextSetBit(pos + 1);
			}
		}        
	}

	public void selectAgeByActivities(int ageOff, int ageEnd, BitSet bs) {
		checkArgument(ageOff < ageEnd);

		for (Map.Entry<String, FieldFilter> entry : ageByFilters.entrySet()) {
			this.filterAgeActivity(ageOff, ageEnd, bs, chunk.getField(entry.getKey()).getValueVector(),
					entry.getValue());
		}

		// age by dimension
		// each qualified activity will make the positions of all neighbouring 
		// activities with the same dimension value set
		int fieldID = tableSchema.getFieldID(query.getAgeField().getField());
		if (fieldID != tableSchema.getActionField() &&
				fieldID != tableSchema.getActionTimeField()) {   
			InputVector inputVector = this.chunk.getField(fieldID).getValueVector();
			int pos = ageOff;
			inputVector.skipTo(pos);
			int lastVal = inputVector.next();
			while (pos < ageEnd) {                
				int oldPos = pos;
				while (++pos < ageEnd) {
					int v = inputVector.next();
					if (v != lastVal) {
						lastVal = v;
						break;
					}
				}
				int setBit = bs.nextSetBit(oldPos);
				if (setBit >= 0 && setBit < pos) {
					bs.set(oldPos, pos);
				}
			}
		}
	}

	/**
	 * Select age activity tuples bounded by [ageOff, ageEnd)
	 * 
	 * @param ageOff
	 *            the start position of age tuples
	 * @param ageEnd
	 *            the end position of age tuples
	 * @param bs
	 *            the hit position list of all qualified tuples
	 * @return the name of the metric field, if it exists.
	 */
	public String selectAgeActivities(int ageOff, int ageEnd, BitSet bs, BitSet ageDelimiters) {
		checkArgument(ageOff < ageEnd);
		// enable the dimension-based ageby operator to be processed in the same way
		// as event-based ageby operator
		int fieldID = tableSchema.getFieldID(query.getAgeField().getField());
		if (fieldID != tableSchema.getActionField() &&
				fieldID != tableSchema.getActionTimeField()) {        	
			bs.and(ageDelimiters);
			int pos = ageDelimiters.nextSetBit(ageOff);
			int lastSetbit = pos;
			InputVector inputVector = this.chunk.getField(fieldID).getValueVector();
			while(pos >= 0 && pos < ageEnd) {
				lastSetbit = pos;
				inputVector.skipTo(pos);
				int lastValue = inputVector.next();                
				while(++pos < ageEnd) {
					if (ageDelimiters.get(pos) && inputVector.next() == lastValue) {
						ageDelimiters.clear(pos);
						lastSetbit = pos;
					}
					else {
						pos = ageDelimiters.nextSetBit(pos);                        
						break;
					}
				}
			}

			// clear the first setbit and set the the bit next to the last set bit
			// so as to enable the resulting delimiters 
			// to be consistent with event-based ageby operator
			if ((pos = ageDelimiters.nextSetBit(ageOff)) >= 0) {
				ageDelimiters.clear(pos);
				checkArgument(!ageDelimiters.get(lastSetbit + 1));
				ageDelimiters.set(lastSetbit + 1);
			}
		}

		// Columnar processing strategy ...
		String metricAgeFilterName = null;
		for (Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
			FieldFilter ageFilter = entry.getValue();
			if(tableSchema.getField(entry.getKey()).getFieldType()==FieldType.Metric) metricAgeFilterName = entry.getKey();
			filterAgeActivity(ageOff, ageEnd, bs, chunk.getField(entry.getKey()).getValueVector(), ageFilter);
		}
		return metricAgeFilterName;
	}

	private Double getBirthAttribute(int baseEvent, int fieldID) {
		FieldSchema schema = tableSchema.getField(fieldID);
		EventAggregator aggregator;

		if (schema.getDataType() != DataType.Aggregate)
			aggregator = BirthAggregatorFactory.getAggregator("UNIQUE");
		else {
			aggregator = BirthAggregatorFactory.getAggregator(schema.getAggregator());
//			fieldID = tableSchema.getFieldID(schema.getBaseField());
		}
		aggregator.init(chunk.getField(fieldID).getValueVector());
		return aggregator.birthAggregate(eventOffset.get(baseEvent));
	}

	public boolean isUserActiveCublet() {
		return this.bUserActiveCublet;
	}

	public boolean isUserActiveChunk() {
		return this.bUserActiveChunk;
	}

	public boolean isAgeActiveChunk() {
		return this.bAgeActiveChunk;
	}

	public FieldFilter getAgeFieldFilters(String atFieldName) {
		return this.ageFilters.get(atFieldName);
	}

	public FieldFilter getAgeFieldFilter() {
		return this.ageFilter;
	}

	public Object getCubletResults() {
		return null;
	}

	public void close() throws IOException {
	}
}
