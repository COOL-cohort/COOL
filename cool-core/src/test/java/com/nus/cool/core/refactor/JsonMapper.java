package com.nus.cool.core.refactor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.CohortProcessor;
import com.nus.cool.core.cohort.refactor.ageSelect.AgeSelection;
import com.nus.cool.core.cohort.refactor.birthSelect.BirthSelection;
import com.nus.cool.core.cohort.refactor.cohortSelect.CohortSelectionLayout;
import com.nus.cool.core.cohort.refactor.cohortSelect.CohortSelector;
import com.nus.cool.core.cohort.refactor.valueSelect.ValueSelection;

public class JsonMapper {
    /**
     * Use the Jackson JSON Tree Model to check the ObjectMapper from Json to
     * CohortQuery
     * 
     * @throws IOException
     */

    @Test
    public void JsonLoadTest() throws IOException {
        Path resourceRoot = Paths.get(System.getProperty("user.dir"),
                "src", "test", "java", "com", "nus", "cool", "core", "resources");
        Path queryPath = Paths.get(resourceRoot.toString(),
                "health", "query_test.json");
        System.out.println("queryPath: " + queryPath.toString());
        File queryFile = new File(queryPath.toString());
        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode cohortQueryJsonNode = objectMapper.readTree(queryFile);

        // check the level_1
        JsonNode birthSelectorJsonNode = cohortQueryJsonNode.get("birthSelector");
        System.out.println(birthSelectorJsonNode.toString());
        // JsonNode birthEventJsonNode = birthSelectorJsonNode.get("birthEvents");

        // birthSelector create
        BirthSelection birthSelector = objectMapper.treeToValue(birthSelectorJsonNode, BirthSelection.class);
        birthSelector.init();
        // pass the BirthSelection

        JsonNode cohortSelectorJsonNode = cohortQueryJsonNode.get("cohortSelector");
        System.out.println(cohortSelectorJsonNode.toString());
        // cohortSelector create
        CohortSelectionLayout cohortSelectLayout = objectMapper.treeToValue(cohortSelectorJsonNode, CohortSelectionLayout.class);
        CohortSelector cohortSelector = cohortSelectLayout.generateCohortSelector();
        

        JsonNode ageSelectorJsonNode = cohortQueryJsonNode.get("ageSelector");
        System.out.println(ageSelectorJsonNode.toString());
        AgeSelection ageSelector = objectMapper.treeToValue(ageSelectorJsonNode, AgeSelection.class);
        ageSelector.init();

        JsonNode valueSelectorJsonNode = cohortQueryJsonNode.get("valueSelector");
        System.out.println(valueSelectorJsonNode.toString());
        ValueSelection valueSelector = objectMapper.treeToValue(valueSelectorJsonNode, ValueSelection.class);
        valueSelector.init();
    
        CohortProcessor cohortProcessor = CohortProcessor.readFromJson(queryFile);
    }

}
