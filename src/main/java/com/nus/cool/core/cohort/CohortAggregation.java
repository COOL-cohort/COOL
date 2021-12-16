package com.nus.cool.core.cohort;

import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.aggregator.Aggregator;
import com.nus.cool.core.cohort.aggregator.SumAggregator;
import com.nus.cool.core.cohort.aggregator.UserCountAggregator;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.RLEInputVector;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import lombok.Getter;

import java.util.BitSet;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CohortAggregation implements Operator {

    private CohortSelection sigma;

    private TableSchema schema;

    private CohortQuery query;

    private String[] birthActions;

    private int[] birthActionGlobalIDs;

    private int[] bBirthActionChunkIDs;

    @Getter
    private BitSet bs;

    @Getter
    private Map<CohortKey, Long> cubletResults = Maps.newLinkedHashMap();

    public CohortAggregation(CohortSelection sigma) {
        this.sigma = checkNotNull(sigma);
    }

    @Override
    public void init(TableSchema schema, CohortQuery query) {
        this.schema = checkNotNull(schema);
        this.query = checkNotNull(query);
        this.birthActions = query.getBirthActions();
        this.sigma.init(schema, query);
    }

    @Override
    public void process(MetaChunkRS metaChunk) {
        this.sigma.process(metaChunk);

        int actionField = this.schema.getActionField();
        MetaFieldRS actionMetaField = metaChunk.getMetaField(actionField, FieldType.Action);
        this.birthActionGlobalIDs = new int[this.birthActions.length];
        for (int i = 0; i < this.birthActions.length; i++) {
            int id = actionMetaField.find(this.birthActions[i]);
            if (id < 0)
                throw new RuntimeException("Unknown birth action: " + this.birthActions[i]);
            this.birthActionGlobalIDs[i] = id;
        }
    }

    @Override
    public void process(ChunkRS chunk) {
        this.sigma.process(chunk);
        if (!this.sigma.isBUserActiveChunk())
            return;

        FieldRS appField = loadField(chunk, this.schema.getAppKeyField());
        FieldRS userField = loadField(chunk, this.schema.getUserKeyField());
        FieldRS actionField = loadField(chunk, this.schema.getActionField());
        FieldRS actionTimeField = loadField(chunk, this.schema.getActionTimeField());
        FieldRS cohortField = loadField(chunk, this.query.getCohortFields()[0]);
        FieldRS metricField = loadField(chunk, this.query.getMetric());
        this.bBirthActionChunkIDs = new int[this.birthActionGlobalIDs.length];
        for (int i = 0; i < this.birthActionGlobalIDs.length; i++) {
            int id = actionField.getKeyVector().find(this.birthActionGlobalIDs[i]);
            if (id < 0)
                return;
            this.bBirthActionChunkIDs[i] = id;
        }

        int min = cohortField.minKey();
        int cardinality = cohortField.maxKey() - min + 1;
        int cohortSize = actionTimeField.maxKey() - actionTimeField.minKey() + 1 + 1;
        long[][] chunkResults = new long[cardinality][cohortSize];

        InputVector cohortInput = cohortField.getValueVector();
        InputVector actionTimeInput = actionTimeField.getValueVector();
        InputVector metricInput = metricField == null ? null : metricField.getValueVector();
        Aggregator aggregator = newAggregator();
        int minAllowedAge = 0;
        int maxAllowedAge = cohortSize - 1;
        aggregator.init(metricInput, actionTimeInput, cohortSize, minAllowedAge, maxAllowedAge, this.query.getAgeInterval());

        RLEInputVector appInput = (RLEInputVector) appField.getValueVector();
        appInput.skipTo(0);
        RLEInputVector.Block appBlock = new RLEInputVector.Block();
        FieldFilter appFilter = this.sigma.getAppFilter();
        BitSet bv = new BitSet(chunk.getRecords());
        this.bs = new BitSet(chunk.getRecords());

        while (appInput.hasNext()) {
            appInput.nextBlock(appBlock);
            if (appFilter.accept(appBlock.value)) {
                if (!(userField.getValueVector() instanceof RLEInputVector))
                    continue;
                RLEInputVector userInput = (RLEInputVector) userField.getValueVector();
                userInput.skipTo(0);
                RLEInputVector.Block userBlock = new RLEInputVector.Block();
                while (userInput.hasNext()) {
                    userInput.nextBlock(userBlock);
                    if (userBlock.off < appBlock.off)
                        continue;
                    if (userBlock.off > appBlock.off + appBlock.len)
                        break;

                    int begin = userBlock.off;
                    int end = userBlock.off + userBlock.len;
                    InputVector actionInput = actionField.getValueVector();
                    actionInput.skipTo(begin);
                    int birthOff = seekToBirthTuple(begin, end, actionInput);
                    if (birthOff == end) {
                        continue;
                    }

                    if (this.sigma.selectUser(birthOff)) {
                        this.bs.set(begin, end);
                        cohortInput.skipTo(birthOff);
                        int cohort = cohortInput.next() - min;
                        chunkResults[cohort][0]++;
                        actionTimeInput.skipTo(birthOff);
                        int birthTime = actionTimeInput.next();
                        int ageOff = birthOff + 1;
                        if (ageOff < end) {
                            bv.set(birthOff + 1, end);
                            this.sigma.selectAgeActivities(birthOff + 1, end, bv);
                            if (!bv.isEmpty())
                                aggregator.processUser(bv, birthTime, birthOff + 1, end, chunkResults[cohort]);
                            bv.clear(birthOff + 1, end);
                        }
                    }
                }
            }
        }

        InputVector keyVector = null;
        if (cohortField.isSetField())
            keyVector = cohortField.getKeyVector();
        for (int i = 0; i < cardinality; i++) {
            if (chunkResults[i][0] > 0) {
                int cohort = keyVector == null ? i + min : keyVector.get(i + min);
                for (int j = 0; j < cohortSize; j++) {
                    CohortKey key = new CohortKey(cohort, j);
                    long value = 0;
                    if (this.cubletResults.containsKey(key))
                        value = this.cubletResults.get(key);
                    if (value + chunkResults[i][j] > 0)
                        this.cubletResults.put(key, value + chunkResults[i][j]);
                }
            }
        }
    }

    private synchronized FieldRS loadField(ChunkRS chunk, int fieldId) {
        return chunk.getField(fieldId);
    }

    private FieldRS loadField(ChunkRS chunk, String fieldName) {
        if ("Retention".equals(fieldName))
            return null;
        int id = this.schema.getFieldID(fieldName);
        return loadField(chunk, id);
    }

    private Aggregator newAggregator() {
        String metric = this.query.getMetric();
        if (metric.equals("Retention"))
            return new UserCountAggregator();
        else
            return new SumAggregator();
    }

    private int seekToBirthTuple(int begin, int end, InputVector actionInput) {
        int pos = begin - 1;
        for (int id : this.bBirthActionChunkIDs) {
            pos++;
            for (; pos < end; pos++) {
                if (actionInput.next() == id) {
                    break;
                }
            }
        }
        return Math.min(pos, end);
    }
}
