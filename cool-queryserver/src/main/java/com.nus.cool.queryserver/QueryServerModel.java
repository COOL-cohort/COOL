package com.nus.cool.queryserver;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.loader.CoolModel;
import com.nus.cool.loader.ExtendedResultTuple;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.File;
import java.util.List;

public class QueryServerModel {
    private CoolModel coolModel;

    private String rootPath;

    private QueryExecutor queryExecutor;

    public QueryServerModel(String datasetPath){
        this.rootPath = datasetPath;
        this.coolModel = new CoolModel(datasetPath);
    }

    public Response reloadCube(String cube) throws IOException {
        this.coolModel.reload(cube);
        return Response.ok().build();
    }

    public Response cohrtAnalysis(ExtendedCohortQuery query) throws IOException {
        if(!query.isValid())
            throw new IOException("Wrong cohort query");

        String inputSource = query.getDataSource();
        CubeRS inputCube = this.coolModel.getCube(inputSource);
        String inputCohort = query.getInputCohort();
        if( inputCohort != null){
            this.coolModel.loadCohorts(inputCohort, this.rootPath+File.separator+inputSource);
        }

        InputVector userVector = this.coolModel.getCohortUsers(query.getInputCohort());
        List<ExtendedResultTuple> results = queryExecutor.executeCohortQuery(inputCube, userVector, query);

        return Response.ok(results).build();
    }

    public Response creatCohort(ExtendedCohortQuery query) throws IOException {
        String inputSource = query.getDataSource();
        CubeRS inputCube = this.coolModel.getCube(inputSource);

        List<Integer> users = queryExecutor.selectCohortUsers(inputCube, null, query);

        String outputCohort = query.getOutputCohort();
        File cohortFile = new File(this.rootPath+File.separator+inputSource, outputCohort);
        if (cohortFile.exists()){
            cohortFile.delete();
            System.out.println("[*] Cohort  " + outputCohort + " exists and is deleted!");
        }
        queryExecutor.createCohort(query, users, this.rootPath+File.separator+inputSource);

        return Response.ok(users).build();
    }
}
