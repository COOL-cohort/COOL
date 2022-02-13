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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nus.cool.core.cohort.ExtendedCohortQuery.AgeField;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.cohort.aggregator.EventAggregator;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.RLEInputVector;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.schema.Measure;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExtendedCohortAggregation implements CohortOperator {

    private static Log LOG = LogFactory.getLog(ExtendedCohortAggregation.class);

    private TableSchema tableSchema;

    private ExtendedCohortSelection sigma;

    private InputVector cohortUsers;

    private int curUser = -1;

    private ExtendedCohortQuery query;

    private int totalDataChunks;

    private int totalSkippedDataChunks;

    private int totalUsers;

    private int totalSkippedUsers;

    private long birthSelectionTime;
    private long ageByTime;
    private long ageSelectionTime;
    private long aggregationTime;

    MetaChunkRS metaChunk;

    Map<ExtendedCohort, Map<Integer, List<Double>>> cubletResults = new HashMap<>();

    public ExtendedCohortAggregation(ExtendedCohortSelection sigma) {
        this.sigma = checkNotNull(sigma);
    }

    public Object getCubletResults() {
        return this.cubletResults;
    }

    @Override
    public void close() throws IOException {
        sigma.close();
        LOG.info(String.format("(birth selection time = %d, age by time = %d, age selection time = %d, aggregation time = %d)",
                this.birthSelectionTime, this.ageByTime, this.ageSelectionTime, this.aggregationTime));
        LOG.info(String.format("(totalChunks = %d, totalSkippedChunks = %d, totalUsers = %d, totalSkippedUsers = %d)",
                totalDataChunks, totalSkippedDataChunks, totalUsers, totalSkippedUsers));
    }

    @Override
    public void init(TableSchema tableSchema, InputVector cohortUsers, ExtendedCohortQuery query) {
        LOG.info("Initializing cohort aggregation operator ...");
        this.tableSchema = checkNotNull(tableSchema);
        this.cohortUsers = cohortUsers;
        this.query = query;
        curUser = -1;
        if (cohortUsers != null && cohortUsers.size() > 0) {
            curUser = cohortUsers.next();
        }
        sigma.init(tableSchema, query);
    }

    @Override
    public void init(TableSchema schema, CohortQuery query) {

    }

    @Override
    public void process(MetaChunkRS metaChunk) {
        LOG.info("Processing metaChunk ...");
        this.metaChunk = metaChunk;
        sigma.process(metaChunk);
    }

    @Override
    public boolean isCohortsInCublet() {
        return true;
    }

    @Override
    public void process(ChunkRS chunk) {
        totalDataChunks++;

        sigma.process(chunk);
        if (!sigma.isUserActiveChunk()) {
            totalSkippedDataChunks++;
            return;
        }

        // Load necessary fields, we need at least four fields
        FieldRS userField = chunk.getField(tableSchema.getUserKeyField());
        FieldRS actionTimeField = chunk.getField(tableSchema.getActionTimeField());

//        Measure measure = cubeSchema.getMeasure(query.getMeasure());
//        checkArgument(measure != null);
//        FieldRS metricField = chunk.getField(measure.getTableFieldName());
//        FieldRS metricField = chunk.getField("id");
        FieldRS metricField = userField;

        // Intentionally allocate one additional bit for dimension-base ageby aggregation
        BitSet bv = new BitSet(chunk.records() + 1);

        AgeField ageField = query.getAgeField();
        BitSet ageDelimiter = null;
        if (tableSchema.getFieldID(ageField.getField()) != tableSchema.getActionTimeField())
            ageDelimiter = new BitSet(chunk.records() + 1);

        // Skipping non RLE compressed blocks
        int totalCorruptedUsers = 0;

        if (userField.getValueVector() instanceof RLEInputVector == false) {
            totalCorruptedUsers++;
            return;
        }

        RLEInputVector userInput = (RLEInputVector) userField.getValueVector();
        RLEInputVector.Block userBlock = new RLEInputVector.Block();
        InputVector userKey = userField.getKeyVector();

        // TODO: later will dynamically determine the scan of cohort users:
        // either do a sequential scan or use the index
        while (userInput.hasNext()) {

            userInput.nextBlock(userBlock); // Next user RLE block

            // Find a new user
            totalUsers++;
            int beg = userBlock.off;
            int end = userBlock.off + userBlock.len;

            if (this.cohortUsers != null) {
                if (curUser != userKey.get(userBlock.value) && curUser >= 0)
                    continue;
                if (cohortUsers.hasNext())
                    curUser = cohortUsers.next();
                else return;
            }

            ExtendedCohort cohort = sigma.selectUser(beg, end);

            if (cohort == null) {
                totalSkippedUsers++;
                continue;
            }


            Map<Integer, List<Double>> cohortCells =
                    (Map<Integer, List<Double>>) cubletResults.get(cohort);

            if (cohortCells == null) {
                cohortCells = new HashMap<>();
                List<Double> cellValue = new ArrayList<>(1);
                cellValue.add(0.0);
                cohortCells.put(0, cellValue);
                cubletResults.put(new ExtendedCohort(cohort), cohortCells);
            }

            cohortCells.get(0).set(0, cohortCells.get(0).get(0) + 1);

            int ageOff = cohort.getBirthOffset();

            if (ageOff < end && sigma.isAgeActiveChunk()) {
                if (tableSchema.getFieldID(ageField.getField()) != tableSchema.getActionTimeField()) {
                    ageDelimiter.set(ageOff, end);
                    sigma.selectAgeByActivities(ageOff, end, ageDelimiter);
                }
                bv.set(ageOff, end);
                sigma.selectAgeActivities(ageOff, end, bv, ageDelimiter, cohortCells);
                // updateStats(sigma.selectAgeActivities(ageOff, end, bv, ageDelimiter), cohortCells.get(0));
                EventAggregator aggr = BirthAggregatorFactory.getAggregator(
                        //cubeSchema.getMeasure(query.getMeasure()).getAggregator().name()
                        query.getMeasure().toUpperCase()
                );
                aggr.init(metricField.getValueVector());

                if (tableSchema.getFieldID(query.getAgeField().getField()) != tableSchema.getActionTimeField()) {
                    aggr.ageAggregate(bv, ageDelimiter, ageOff, end, this.query.getAgeField().getAgeInterval(),
                            sigma.getAgeFieldFilter(), cohortCells);
                    ageDelimiter.clear(ageOff, end + 1);
                }
                else
                    aggr.ageAggregate(bv, actionTimeField.getValueVector(), cohort.getBirthDate(), ageOff, end,
                            query.getAgeField().getAgeInterval(), query.getAgeField().getUnit(), sigma.getAgeFieldFilter(), cohortCells);
                bv.clear(ageOff, end + 1);
            }
        }

        if (totalCorruptedUsers > 0)
            LOG.info("Total corrupted users: " + totalCorruptedUsers + " " + totalDataChunks);
    }

    private void updateStats(List<Double> newStats, List<Double> cohortCellsStats) {
        // 4 values: min, max, avg, num
        if (cohortCellsStats.size() < 5) {
            for (Double val : newStats)
                cohortCellsStats.add(val);
        }
        else {
            // min
            if (cohortCellsStats.get(1) > newStats.get(0))
                cohortCellsStats.set(1, newStats.get(0));
            // max
            if (cohortCellsStats.get(2) > newStats.get(1))
                cohortCellsStats.set(2, newStats.get(1));
            // avg and num
            cohortCellsStats.set(3, cohortCellsStats.get(3) + newStats.get(2));
            cohortCellsStats.set(4, cohortCellsStats.get(4) + newStats.get(3));
        }
    }
}
