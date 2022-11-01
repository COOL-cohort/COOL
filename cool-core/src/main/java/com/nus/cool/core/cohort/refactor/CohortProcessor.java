package com.nus.cool.core.cohort.refactor;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.ageselect.AgeSelection;
import com.nus.cool.core.cohort.refactor.birthselect.BirthSelection;
import com.nus.cool.core.cohort.refactor.cohortselect.CohortSelector;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;
import com.nus.cool.core.cohort.refactor.utils.DateUtils;
import com.nus.cool.core.cohort.refactor.valueselect.ValueSelection;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;

import lombok.Getter;

public class CohortProcessor {

    private final AgeSelection ageSelector;

    private final ValueSelection valueSelector;

    private final CohortSelector cohortSelector;

    private final BirthSelection birthSelector;

    @Getter
    private final String dataSource;

    private ProjectedTuple tuple;

    @Getter
    private final CohortRet result;

    private String UserIdSchema;

    private String ActionTimeSchema;

    private final HashSet<String> projectedSchemaSet;

    public CohortProcessor(CohortQueryLayout layout) {

        this.ageSelector = layout.getAgetSelectionLayout().generate();
        this.birthSelector = layout.getBirthSelectionLayout().generate();
        this.cohortSelector = layout.getCohortSelectionLayout().generate();
        this.valueSelector = layout.getValueSelectionLayout().generate();

        this.projectedSchemaSet = layout.getSchemaSet();
        this.dataSource = layout.getDataSource();
        this.result = new CohortRet(layout.getAgetSelectionLayout());
    }

    /**
     * Public interface, Scan whole table and return CohortResult
     *
     * @param cube
     * @return CohortRet
     */
    public CohortRet process(CubeRS cube) throws IOException {
        // initialize the UserId and KeyId
        TableSchema tableschema = cube.getSchema();
        for (FieldSchema fieldSchema : tableschema.getFields()) {
            if (fieldSchema.getFieldType() == FieldType.UserKey) {
                this.UserIdSchema = fieldSchema.getName();
            } else if (fieldSchema.getFieldType() == FieldType.ActionTime) {
                this.ActionTimeSchema = fieldSchema.getName();
            }
        }
        // add this two schema into List
        this.projectedSchemaSet.add(this.UserIdSchema);
        this.projectedSchemaSet.add(this.ActionTimeSchema);
        this.tuple = new ProjectedTuple(this.projectedSchemaSet);
        for (CubletRS cublet : cube.getCublets()) {
            processCublet(cublet);
        }
        return this.result;
    }

    /**
     * Process one Cublet
     *
     * @param cublet
     */
    private void processCublet(CubletRS cublet) {
        MetaChunkRS metaChunk = cublet.getMetaChunk();
        // transfer values in SetFilter into GloablId
        this.FilterInit(metaChunk);

        // use MetaChunk to skip Cublet
        if (!this.checkMetaChunk(metaChunk)) {
            return;
        }

        // Now start to pass the DataChunk
        for (ChunkRS chunk : cublet.getDataChunks()) {
            if (this.checkDataChunk(chunk)) {
                this.processDataChunk(chunk, metaChunk);
            }
        }
    }

    /**
     * In this section, we load the tuple which is an inner property.
     * We left the process logic in processTuple function.
     *
     * @param chunk              dataChunk
     * @param metaChunk          metaChunk
     * @param hashMapperBySchema map of filedName: []value
     * @param invariantGidMap    map of invariant filedName: []value
     */
    private void processDataChunk(ChunkRS chunk, MetaChunkRS metaChunk) {
        for (int i = 0; i < chunk.getRecords(); i++) {
            // load data into tuple
            for (String schema : this.projectedSchemaSet) {
                int value = chunk.getField(schema).getValueByIndex(i);
                this.tuple.loadAttr(value, schema);
            }

            this.processTuple(metaChunk);
        }

    }

    /**
     * process the inner tuple
     */
    private void processTuple(MetaChunkRS metaChunk) {
        // For One Tuple, we firstly get the userId, and ActionTime
        int userGlobalID = (int) tuple.getValueBySchema(this.UserIdSchema);
        MetaFieldRS userMetaField = metaChunk.getMetaField(this.UserIdSchema);
        String userId = userMetaField.getString(userGlobalID);

        LocalDateTime actionTime = DateUtils.daysSinceEpoch((int) tuple.getValueBySchema(this.ActionTimeSchema));
        // check whether its birthEvent is selected
        if (!this.birthSelector.isUserSelected(userId)) {
            // if birthEvent is not selected
            this.birthSelector.selectEvent(userId, actionTime, this.tuple);
        } else {
            // the birthEvent is selected
            // do time_diff to generate age / get the BirthEvent Date
            LocalDateTime birthTime = this.birthSelector.getUserBirthEventDate(userId);
            int age = this.ageSelector.generateAge(birthTime, actionTime);
            if (age == AgeSelection.DefaultNullAge) {
                // age is outofrange
                return;
            }
            // extract the cohort this tuple belong to
            String cohortName = this.cohortSelector.selectCohort(this.tuple, metaChunk);
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
            RetUnit ret = this.result.getByAge(cohortName, age);

            this.valueSelector.getAggregateFunc().calculate(ret, tuple);
        }
    }

    /**
     * Check if this cublet contains the required field.
     * 
     * @param metaChunk hashMetaFields result
     * @return true: this metaChunk is valid, false: this metaChunk is invalid.
     */
    private Boolean checkMetaChunk(MetaChunkRS metaChunk) {

        // 1. check birth selection
        // if the metaChunk contains all birth filter's accept value, then the metaChunk
        // is valid.
        if (this.birthSelector.getBirthEvents() == null) {
            return true;
        }

        for (Filter filter : this.birthSelector.getFilterList()) {
            String checkedSchema = filter.getFilterSchema();
            MetaFieldRS metaField = metaChunk.getMetaField(checkedSchema);
            if (this.checkMetaField(metaField, filter))
                return true;
        }

        // 2. check birth selection
        Filter cohortFilter = this.cohortSelector.getFilter();
        String checkedSchema = cohortFilter.getFilterSchema();
        MetaFieldRS metaField = metaChunk.getMetaField(checkedSchema);
        if (this.checkMetaField(metaField, cohortFilter)) {
            return true;
        }

        // 3. check value Selector,
        for (Filter ft : this.valueSelector.getFilterList()) {
            String ValueSchema = ft.getFilterSchema();
            MetaFieldRS ValueMetaField = metaChunk.getMetaField(ValueSchema);
            if (this.checkMetaField(ValueMetaField, ft)) {
                return true;
            }
        }
        return false;
    }

    public Boolean checkMetaField(MetaFieldRS metaField, Filter ft) {
        // if(ft == FilterType.Range){
        // Scope scope = new Scope(metaField.getMinValue(), metaField.getMaxValue());
        // return ft.accept(scope);
        // }
        return true;
    }

    /***
     *
     * @param chunk data chunk
     * @return if this data chunk need to check
     */
    public Boolean checkDataChunk(ChunkRS chunk) {
        return true;
    }

    /**
     * When new Cublet is coming, transfer the set value in Filter to GlobalID
     * 
     * @param metaChunkRS
     */
    private void FilterInit(MetaChunkRS metaChunkRS) {
        // init birthSelector
        for (Filter filter : this.birthSelector.getFilterList()) {
            filter.loadMetaInfo(metaChunkRS);
        }

        // init cohort
        this.cohortSelector.getFilter().loadMetaInfo(metaChunkRS);

        // value age
        for (Filter filter : this.valueSelector.getFilterList()) {
            filter.loadMetaInfo(metaChunkRS);
        }
    }

    /**
     * Read from json file and create a instance of CohortProcessor
     * 
     * @param in File
     * @return instance of file
     * @throws IOException IOException
     */
    public static CohortProcessor readFromJson(File in) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        CohortProcessor instance = mapper.readValue(in, CohortProcessor.class);
        return instance;
    }

    public static CohortProcessor readFromJson(String path) throws IOException {
        return readFromJson(new File(path));
    }

}