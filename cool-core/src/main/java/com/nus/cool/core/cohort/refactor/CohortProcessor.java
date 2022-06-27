package com.nus.cool.core.cohort.refactor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.ageSelect.AgeSelection;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFactory;
import com.nus.cool.core.cohort.refactor.aggregate.AggregateFunc;
import com.nus.cool.core.cohort.refactor.birthSelect.BirthSelection;
import com.nus.cool.core.cohort.refactor.cohortSelect.CohortSelectionLayout;
import com.nus.cool.core.cohort.refactor.cohortSelect.CohortSelector;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;
import com.nus.cool.core.cohort.refactor.utils.DateUtils;
import com.nus.cool.core.cohort.refactor.valueSelect.ValueSelection;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.HashMetaFieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.FieldType;

import javafx.util.Pair;
import lombok.Getter;

public class CohortProcessor {

    private AgeSelection ageSelector;

    @JsonProperty("cohortSelector")
    private CohortSelectionLayout cohortSelectionLayout;

    private ValueSelection valueSelector;

    @JsonIgnore
    private CohortSelector cohortSelector;

    private BirthSelection birthSelector;

    @JsonIgnore
    private List<String> projectedSchemaList;

    @JsonIgnore
    private boolean inilialize = false;

    @JsonIgnore
    private final String UserIdSchema = "UserID";

    @JsonIgnore
    private final String ActionTimeSchema = "ActionTime";

    @JsonIgnore
    private ProjectedTuple tuple;

    @Getter
    @JsonIgnore
    private CohortRet result;

    /**
     * Create some filter instance
     */
    public void init() {
        this.inilialize = true;
        this.ageSelector.init();
        this.cohortSelector = this.cohortSelectionLayout.generateCohortSelector();
        this.birthSelector.init();
        // Merge Schema
        HashSet<String> schemaMap = new HashSet<>();
        // Default Add UserIdSchema and ActionTimeSchema
        schemaMap.add(this.UserIdSchema);
        schemaMap.add(this.ActionTimeSchema);
        schemaMap.add(this.cohortSelector.getSchema());
        schemaMap.addAll(this.valueSelector.getSchemaList());
        schemaMap.addAll(this.birthSelector.getRelatedSchemas());
        this.projectedSchemaList = new ArrayList<>(schemaMap);
        this.tuple = new ProjectedTuple(this.projectedSchemaList);

        // generate CohortResult to store intermediate result
        this.result = new CohortRet(this.ageSelector);
    }

    /**
     * Read from json file and create a instance of CohortProcessor
     * 
     * @param in
     * @return
     * @throws IOException
     */
    public static CohortProcessor readFromJson(File in) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        CohortProcessor instance = mapper.readValue(in, CohortProcessor.class);
        instance.init();
        return instance;
    }

    /**
     * Public interface, Scan whole table and return CohortResult
     * 
     * @param cube
     * @return CohortRet
     */
    public CohortRet process(CubeRS cube) {
        for (CubletRS cublet : cube.getCublets()) {
            processCublet(cublet);
        }
        return null;
    }

    /**
     * Process one Cublet
     * 
     * @param cublet
     */
    private void processCublet(CubletRS cublet) {
        MetaChunkRS metaChunk = cublet.getMetaChunk();
        if (!this.checkMetaChunk(metaChunk)) {
            return;
        }
        // if it is necessary, add logic in method checkMetaChunk
        // Personally, this step is not universal
        // all right, can check whether this cublet pass rangeFilter

        // for Hash Value, Maintain HashMap<Schema, String[]>, the String[] is gid ->
        // True Value
        HashMap<String, String[]> gidMapBySchema = new HashMap<>();

        // we only get used schema;
        for (String schema : this.projectedSchemaList) {
            MetaFieldRS metaField = metaChunk.getMetaField(schema);
            if (FieldType.IsHashType(metaField.getFieldType())) {
                gidMapBySchema.put(schema, ((HashMetaFieldRS) metaField).getGidMap());
            }
        }

        // Now start to pass the DataChunk
        for (ChunkRS chunk : cublet.getDataChunks()) {
            this.processDataChunk(chunk, gidMapBySchema);
        }
    }

    /**
     * In this section, we load the tuple which is a inner property.
     * We left the process logic in procesTuple function.
     * 
     * @param chunk
     * @param hashMapperBySchema
     */
    private void processDataChunk(ChunkRS chunk, HashMap<String, String[]> hashMapperBySchema) {
        for (int i = 0; i < chunk.getRecords(); i++) {
            // load data into tuple
            for (String schema : this.projectedSchemaList) {
                tuple.loadAttr(chunk.getField(schema).getValueByIndex(i), schema);
            }
            // now this tuple is loaded
            this.processTuple();
        }
    }

    /**
     * process the inner tuple
     */
    private void processTuple() {
        // For One Tuple, we firstly get the userId, and ActionTime
        int userId = (int) tuple.getValueBySchema(this.UserIdSchema);
        Calendar actionTime = DateUtils.createCalender((long) tuple.getValueBySchema(this.ActionTimeSchema));
        // check whether its birthEvent is selected
        if (!this.birthSelector.isUserSelected(userId)) {
            // if birthEvent is not selected
            this.birthSelector.selectEvent(userId, actionTime, this.tuple);
        } else {
            // the birthEvent is selected

            // do time_diff to generate age
            // get the BirthEvent Date
            Calendar birthTime = this.birthSelector.getUserBirthEventDate(userId);
            int age = this.ageSelector.generateAge(birthTime, actionTime);
            if (age == AgeSelection.DefaultNullAge) {
                // age is outofrange
                return;
            }

            // extract the cohort this tuple belong to
            String cohortName = this.cohortSelector.selectCohort(this.tuple);
            if (cohortName == null) {
                // cohort is outofrange
                return;
            }

            if (!this.valueSelector.IsSelected(this.tuple)) {
                // value outofrange
                return;
            }
            // Pass all above filter, we can store value into CohortRet
            // get the temporay result for this CohortGroup and this age
            RetUnit ret = this.result.get(cohortName, age);
            // update
            this.valueSelector.updateRetUnit(ret, tuple);
        }
    }

    private boolean checkMetaChunk(MetaChunkRS meta) {
        return true;
    }
}
