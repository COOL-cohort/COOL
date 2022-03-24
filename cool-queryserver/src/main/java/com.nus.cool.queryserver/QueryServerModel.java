package com.nus.cool.queryserver;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.model.CoolCohortEngine;
import com.nus.cool.model.CoolModel;
import com.nus.cool.result.ExtendedResultTuple;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.File;
import java.util.List;

public class QueryServerModel {
    private CoolModel coolModel;

    private String rootPath;

    private CoolCohortEngine cohortEngine = new CoolCohortEngine();

    public QueryServerModel(String datasetPath){
        this.rootPath = datasetPath;
        try{
            this.coolModel = new CoolModel(datasetPath);
        } catch (IOException e){
            System.out.println(e);
        }
    }

    public Response reloadCube(String cube){
        try{
            this.coolModel.reload(cube);
            return Response.ok("Cube " + cube + " is reloaded.").build();
        } catch (IOException e){
            System.out.println(e);
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    public Response creatCohort(ExtendedCohortQuery query){
        try {
            String inputSource = query.getDataSource();
            CubeRS inputCube = this.coolModel.getCube(inputSource);

            List<Integer> users = cohortEngine.selectCohortUsers(inputCube, null, query);

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
            cohortEngine.createCohort(query, users, cohortRoot);
            return Response.ok().entity(users).build();
        } catch (IOException e){
            System.out.println(e);
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    public Response cohrtAnalysis(ExtendedCohortQuery query){
        try {
            if (!query.isValid())
                throw new IOException("[x] Invalid cohort query.");

            String inputSource = query.getDataSource();
            this.coolModel.reload(inputSource);
            CubeRS inputCube = this.coolModel.getCube(inputSource);
            String inputCohort = query.getInputCohort();
            if (inputCohort != null) {
                this.coolModel.loadCohorts(inputCohort, coolModel.getCubeStorePath(inputSource));
            }
            System.out.println(inputCohort);
            InputVector userVector = this.coolModel.getCohortUsers(inputCohort);
            List<ExtendedResultTuple> results = cohortEngine.performCohortQuery(inputCube, userVector, query);

            return Response.ok(results).build();
        } catch (IOException e){
            System.out.println(e);
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    public Response listCubes() {
        return Response.ok().entity(this.coolModel.listCubes()).build();
    }

    public Response listCohorts(String cube) {
        return Response.ok().entity(this.coolModel.listCohorts(cube)).build();
    }
}
