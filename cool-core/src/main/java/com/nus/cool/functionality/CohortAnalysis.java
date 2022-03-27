package com.nus.cool.functionality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.result.ExtendedResultTuple;
import com.nus.cool.model.CoolModel;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CohortAnalysis {
    /**
     * perform cohort query to conduct cohort analysis
     *
     * @param args [0] dataset path: the path to all datasets, e.g., datasetSource
     *        args [1] query path: the path to the cohort query, e.g., health/query2.json
     */
    public static void main(String[] args) {
        String datasetPath = args[0];
        String queryPath = args[1];

        try {
            ObjectMapper mapper = new ObjectMapper();
            ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

            String inputSource = query.getDataSource();
            CoolModel coolModel = new CoolModel(datasetPath);
            coolModel.reload(inputSource);

            if (!query.isValid())
                throw new IOException("[x] Invalid cohort query.");

            CubeRS inputCube = coolModel.getCube(query.getDataSource());
            String inputCohort = query.getInputCohort();
            if (inputCohort != null) {
                coolModel.loadCohorts(inputCohort, inputSource);
                System.out.println("Input cohort: " + inputCohort);
            }
            InputVector userVector = coolModel.getCohortUsers(inputCohort);
            List<ExtendedResultTuple> result = coolModel.cohortEngine.performCohortQuery(inputCube, userVector, query);
            System.out.println("Result for the query is  " + result);
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
