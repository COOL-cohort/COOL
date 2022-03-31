package com.nus.cool.queryserver;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.funnel.FunnelQuery;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.util.writer.DataWriter;
import com.nus.cool.core.util.writer.ListDataWriter;
import com.nus.cool.model.CoolCohortEngine;
import com.nus.cool.model.CoolModel;
import com.nus.cool.result.ExtendedResultTuple;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
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
            if (!this.coolModel.isCubeLoaded(cube)){
                this.coolModel.reload(cube);
                return Response.ok("Cube " + cube + " is reloaded.").build();
            } else return Response.ok("Cube " + cube + " has already been reloaded.").build();
        } catch (IOException e){
            System.out.println(e);
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    public Response cohortSelection(ExtendedCohortQuery query){
        try {
            String inputSource = query.getDataSource();
            this.reloadCube(inputSource);
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

    public Response cohortAnalysis(ExtendedCohortQuery query){
        try {
            if (!query.isValid())
                throw new IOException("[x] Invalid cohort query.");

            String inputSource = query.getDataSource();
            this.coolModel.reload(inputSource);
            CubeRS inputCube = this.coolModel.getCube(inputSource);
            String inputCohort = query.getInputCohort();
            if (inputCohort != null) {
                System.out.println("Input cohort: " + inputCohort);
                this.coolModel.loadCohorts(inputCohort, inputSource);
            }
            InputVector userVector = this.coolModel.getCohortUsers(inputCohort);
            List<ExtendedResultTuple> results = cohortEngine.performCohortQuery(inputCube, userVector, query);
            System.out.println("Result for the query is  " + results);

            return Response.ok(results).build();
        } catch (IOException e){
            System.out.println(e);
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    public Response funnelAnalysis(FunnelQuery query){
        try {
            if (!query.isValid())
                throw new IOException("[x] Invalid cohort query.");

            String inputSource = query.getDataSource();
            this.coolModel.reload(inputSource);

            CubeRS inputCube = coolModel.getCube(query.getDataSource());
            String inputCohort = query.getInputCohort();
            if (inputCohort != null) {
                System.out.println("Input cohort: " + inputCohort);
                coolModel.loadCohorts(inputCohort, inputSource);
            }
            InputVector userVector = coolModel.getCohortUsers(inputCohort);
            int[] results = coolModel.cohortEngine.performFunnelQuery(inputCube, userVector, query);
            System.out.println("Result for the query is  " + Arrays.toString(results));

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
        try {
            String[] cohorts = this.coolModel.listCohorts(cube);
            return Response.ok().entity(cohorts).build();
        } catch (IOException e){
            System.out.println(e);
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }


    public Response cohortExploration(String cube, String cohort) {
        try{
            // load cube
            coolModel.reload(cube);
            CubeRS inputCube = coolModel.getCube(cube);

            // load cohort
            coolModel.loadCohorts(cohort, cube);
            InputVector userVector = coolModel.getCohortUsers(cohort);

            // export cohort
            List<String> results = new ArrayList<String>();
            DataWriter writer = new ListDataWriter(results);
            coolModel.cohortEngine.exportCohort(inputCube, userVector, writer);

            coolModel.close();
            return Response.ok(results).build();
        }catch (IOException e){
            System.out.println(e);
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }

    }

    public Response precessIcebergQuery(IcebergQuery query) {
        try{
            String inputSource = query.getDataSource();
            this.coolModel.reload(inputSource);

            List<BaseResult> results = coolModel.olapEngine.performOlapQuery(coolModel.getCube(inputSource), query);
            System.out.println(results);

            return Response.ok(results).build();
        } catch (Exception e){
            System.out.println(e);
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }
}
