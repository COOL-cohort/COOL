package com.nus.cool.functionality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.funnel.FunnelQuery;

import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.model.CoolModel;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FunnelAnalysis {

    public static void main(String[] args) {
        String datasetPath = args[0];
        String queryPath = args[1];

        try {
            ObjectMapper mapper = new ObjectMapper();
            FunnelQuery query = mapper.readValue(new File(queryPath), FunnelQuery.class);

            if (!query.isValid())
                throw new IOException("[x] Invalid funnel query.");

            String inputSource = query.getDataSource();
            CoolModel coolModel = new CoolModel(datasetPath);
            coolModel.reload(inputSource);

            String inputCohort = query.getInputCohort();
            if (inputCohort != null) {
                coolModel.loadCohorts(inputCohort, inputSource);
                System.out.println("Input cohort: " + inputCohort);
            }

            InputVector userVector = coolModel.getCohortUsers(query.getInputCohort());

            int[] result = coolModel.cohortEngine.performFunnelQuery(coolModel.getCube(inputSource),userVector,query);
            System.out.println(Arrays.toString(result));
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
