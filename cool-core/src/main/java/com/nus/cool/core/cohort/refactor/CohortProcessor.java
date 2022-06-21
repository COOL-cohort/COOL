package com.nus.cool.core.cohort.refactor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.ageSelect.AgeSelection;
import com.nus.cool.core.cohort.refactor.birthSelect.BirthSelection;
import com.nus.cool.core.cohort.refactor.cohortSelect.CohortSelectionLayout;
import com.nus.cool.core.cohort.refactor.cohortSelect.CohortSelector;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.valueSelect.ValueSelection;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.HashMetaFieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.FieldType;

public class CohortProcessor {

    private AgeSelection ageSelector;

    private CohortSelectionLayout cohortSelectionLayout;

    private ValueSelection valueSelector;

    @JsonIgnore
    private CohortSelector cohortSelector;

    private BirthSelection birthSelector;

    @JsonIgnore
    private List<String> projectedSchemaList;

    @JsonIgnore
    private boolean inilialize =  false;
    
    @JsonIgnore
    private final String UserIdSchema = "UserID";
    
    @JsonIgnore
    private final String ActionTimeSchema = "ActionTime";

    @JsonIgnore
    private ProjectedTuple tuple;

    /**
     * Create some filter instance
     */
    public void init(){
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
    }

    /**
     * Read from json file and create a instance of CohortProcessor
     * @param in
     * @return
     * @throws IOException
     */
    public static CohortProcessor readFromJson(InputStream in) throws IOException{
        ObjectMapper mapper = new ObjectMapper();
        CohortProcessor instance = mapper.readValue(in, CohortProcessor.class);
        instance.init();
        return instance;
    }

    /**
     * Public interface, Scan whole table and return CohortResult
     * @param cube
     * @return CohortRet
     */
    public CohortRet process(CubeRS cube){
        for(CubletRS cublet : cube.getCublets()){
            processCublet(cublet);
        }
        return null;
    }

    /**
     * Process one Cublet
     * @param cublet
     */
    private void processCublet(CubletRS cublet){
        MetaChunkRS metaChunk = cublet.getMetaChunk();
        if(!this.checkMetaChunk(metaChunk)){return;}
        // if it is necessary, add logic in method checkMetaChunk
        // Personally, this step is not universal
        // all right, can check whether this cublet pass rangeFilter

        // for Hash Value, Maintain HashMap<Schema, String[]>, the String[] is gid -> True Value
        HashMap<String, String[]> gidMapBySchema =  new HashMap<>();
        
        // we only get used schema;
        for(String schema: this.projectedSchemaList){
            MetaFieldRS metaField = metaChunk.getMetaField(schema);
            if(IsHashType(metaField.getFieldType())){
                gidMapBySchema.put(schema, ((HashMetaFieldRS)metaField).getGidMap());
            }
        }

        // Now start to pass the DataChunk
        for(ChunkRS chunk: cublet.getDataChunks()){
            this.processDataChunk(chunk,gidMapBySchema);
        }
    }

    private void processDataChunk(ChunkRS chunk, HashMap<String, String[]> hashMapperBySchema){
        // maintain a collection fields
        HashMap<String, FieldRS> fieldRSBySchema = new HashMap<>();
        for(String schema:this.projectedSchemaList){
            fieldRSBySchema.put(schema, chunk.getField(schema));
        }

        for(int i = 0; i < chunk.getRecords(); i++){
            // load data into tuple
            for(String schema: this.projectedSchemaList){
                // tuple.loadAttr(, schema);
            }
        }
    }

    
    private boolean IsHashType(FieldType fieldType) {
        return false;
    }

    private boolean checkMetaChunk(MetaChunkRS meta){
        return true;
    }
}
