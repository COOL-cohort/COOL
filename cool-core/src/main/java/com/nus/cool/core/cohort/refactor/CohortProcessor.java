package com.nus.cool.core.cohort.refactor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.ageSelect.AgeSelection;
import com.nus.cool.core.cohort.refactor.birthSelect.BirthSelection;
import com.nus.cool.core.cohort.refactor.cohortSelect.CohortSelectionLayout;
import com.nus.cool.core.cohort.refactor.cohortSelect.CohortSelector;
import com.nus.cool.core.cohort.refactor.valueSelect.ValueSelection;

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

    // public void proces
}
