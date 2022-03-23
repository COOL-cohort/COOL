package com.nus.cool.functionality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.model.CoolModel;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CohortSelection {
    /**
     * perform the cohort query to select users into a cohort
     *
     * @param args [0] dataset path: the path to all datasets, e.g., datasetSource
     *        args [1] query path: the path to the cohort query, e.g., health/query2.json
     */
    public static void main(String[] args) throws IOException {
        String datasetPath = args[0];
        String queryPath = args[1];

        try{
            ObjectMapper mapper = new ObjectMapper();
            ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

            String inputSource = query.getDataSource();
            CoolModel coolModel = new CoolModel(datasetPath);
            coolModel.reload(inputSource);

            CubeRS cube = coolModel.getCube(query.getDataSource());

            List<Integer> cohorResults = coolModel.cohortEngine.selectCohortUsers(cube,null, query);
            System.out.println("Result for query is  " + cohorResults);
            List<String> userIDs = coolModel.cohortEngine.listCohortUsers(cube, cohorResults);
            System.out.println("Actual user IDs are  " + userIDs);

            String outputCohort = query.getOutputCohort();
            File cohortRoot =  new File(coolModel.getCubeStorePath(inputSource), "cohort");
            if(!cohortRoot.exists()){
                cohortRoot.mkdir();
                System.out.println("[*] Cohort Fold " + cohortRoot.getName() + " is created.");
            }
            File cohortFile = new File(cohortRoot, outputCohort);
            if (cohortFile.exists()){
                cohortFile.delete();
                System.out.println("[*] Cohort " + outputCohort + " exists and is deleted!");
            }

            coolModel.cohortEngine.createCohort(query, cohorResults, cohortRoot);
            System.out.println("[*] Cohort results are stored into " + cohortRoot.getAbsolutePath());
        } catch (IOException e){
            System.out.println(e);
        }
    }
}
