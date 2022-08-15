package com.nus.cool.core.cohort.refactor;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.ageSelect.AgeSelection;
import com.nus.cool.core.cohort.refactor.birthSelect.BirthSelection;
import com.nus.cool.core.cohort.refactor.cohortSelect.CohortSelector;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;
import com.nus.cool.core.cohort.refactor.utils.DateUtils;
import com.nus.cool.core.cohort.refactor.valueSelect.ValueSelection;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.HashMetaFieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;

import lombok.Getter;

public class CohortProcessor {

    private AgeSelection ageSelector;

    private ValueSelection valueSelector;

    private CohortSelector cohortSelector;

    private BirthSelection birthSelector;

    @Getter
    private String dataSource;

    private ProjectedTuple tuple;

    @Getter
    private CohortRet result;

    private String UserIdSchema;

    private String ActionTimeSchema;
    
    private HashSet<String> projectedSchemaSet;

    public CohortProcessor(CohortQueryLayout layout){
        this.ageSelector = layout.getAgetSelectionLayout().generate();
        this.birthSelector = layout.getBirthSelectionLayout().generate();
        this.cohortSelector = layout.getCohortSelectionLayout().generate();
        this.valueSelector = layout.getValueSelectionLayout().generate();
        this.projectedSchemaSet = layout.getSchemaSet();
        this.dataSource = layout.getDataSource();
        this.result =  new CohortRet(layout.getAgetSelectionLayout());
    }

   
    /**
     * Public interface, Scan whole table and return CohortResult
     * 
     * @param cube
     * @return CohortRet
     */
    public CohortRet process(CubeRS cube) {
        // initialize the UserId and KeyId
        TableSchema tableschema = cube.getSchema();
        for(FieldSchema fieldSchema : tableschema.getFields()){
            if(fieldSchema.getFieldType() == FieldType.UserKey){
                this.UserIdSchema = fieldSchema.getName();
            } else if (fieldSchema.getFieldType() == FieldType.ActionTime){
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
        for (String schema : this.projectedSchemaSet) {
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
            for (String schema : this.projectedSchemaSet) {
                // if the value is segment type, we should convert it to String from globalId
                if(hashMapperBySchema.containsKey(schema)){
                    int globalId = chunk.getField(schema).getValueByIndex(i);
                    String v = hashMapperBySchema.get(schema)[globalId];
                    tuple.loadAttr(v, schema);
                } else {
                    tuple.loadAttr(chunk.getField(schema).getValueByIndex(i), schema);
                }
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
        String userId = (String) tuple.getValueBySchema(this.UserIdSchema);
        LocalDateTime actionTime = DateUtils.daysSinceEpoch((int)tuple.getValueBySchema(this.ActionTimeSchema));
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
//            System.out.println("[Update Cohort Result]: cohortName:" + cohortName + "\tage:"+ age);
            RetUnit ret = this.result.getByAge(cohortName, age);
            // update
            this.valueSelector.updateRetUnit(ret, tuple);
        }
    }

    private boolean checkMetaChunk(MetaChunkRS meta) {
        return true;
    }



    /**-------------------  IO Factory    ----------------------*/
     /**
     * Read from json file and create a instance of CohortProcessor
     * 
     * @param in File
     * @return
     * @throws IOException
     */
    public static CohortProcessor readFromJson(File in) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        CohortProcessor instance = mapper.readValue(in, CohortProcessor.class);
        return instance;
    }


    public static CohortProcessor readFromJson(String path) throws IOException{
        return readFromJson(new File(path));
    }

}
