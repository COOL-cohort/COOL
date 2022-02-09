package com.nus.cool.loader;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.CohortUserSection;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.ExtendedCohortSelection;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.ZIntBitCompressor;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.TableSchema;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CreateCohort {

    public static void main(String[] args) throws IOException {

        String testPath = args[0];
        String dataPath = args[1];
        String queryPath = args[2];

        CoolModel coolModel = new CoolModel(testPath);
        coolModel.reload(dataPath);

        // cohort query from json
        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery query = mapper.readValue(new File(queryPath), ExtendedCohortQuery.class);

        System.out.println(" ------  checking query info ------ ");
        System.out.println(query);
        System.out.println(" ------  checking query info done ------ ");

        String cohort = query.getOutputCohort();
        if (cohort != null){
            throw new IOException("Output cohort exists");
        }

        QueryResult result = selectCohortUsers(coolModel.getCube(query.getDataSource()),null, query);

        System.out.println(" result for query0 is  " + result);

        // materialize to a cohort store
        try {
            createCohort(query, (List<Integer>) result.getResult(), testPath);

        } catch (IOException e) {
            throw new IOException("Output cohort exists");
        }
    }

    public static QueryResult selectCohortUsers(CubeRS cube,
                                                InputVector users,
                                                ExtendedCohortQuery query) throws IOException {
        if (cube == null)
            throw new IOException("data source is null");

        List<CubletRS> cublets = cube.getCublets();
        TableSchema tableSchema = cube.getSchema();
        List<Integer> userList = new ArrayList<>();

        for (CubletRS cubletRS : cublets) {
            MetaChunkRS metaChunk = cubletRS.getMetaChunk();
            ExtendedCohortSelection sigma = new ExtendedCohortSelection();
            CohortUserSection gamma = new CohortUserSection(sigma);
            gamma.init(tableSchema, users, query);
            gamma.process(metaChunk);
            if(sigma.isUserActiveCublet()) {
                List<ChunkRS> dataChunks = cubletRS.getDataChunks();
                for(ChunkRS dataChunk : dataChunks) {
                    gamma.process(dataChunk);
                }
            }

            userList.addAll(gamma.getCubletResults());
        }

        return QueryResult.ok(userList);
    }


    public static void createCohort(ExtendedCohortQuery query,
                                          List<Integer> users,
                                          String testPath) throws IOException {

        String cohortName = query.getOutputCohort();
        File cohort = new File(new File(testPath), cohortName);

        cohort.createNewFile();

        DataOutputStream stream =
                new DataOutputStream(new FileOutputStream(cohort));
        int[] userArray = new int[users.size()];
        Iterator<Integer> iter = users.iterator();
        for (int i = 0; i < userArray.length; i++) {
            userArray[i] = iter.next().intValue();
        }
        Compressor compressor = new ZIntBitCompressor(
                Histogram.builder()
                        .max(max(userArray))
//                        .count(userArray.length)
//                        .uniqueValues(userArray.length)
                        .build());
        byte[] compressed = new byte[compressor.maxCompressedLength()];
        int nbytes = compressor.compress(userArray, 0, userArray.length,
                compressed, 0, compressed.length);
        stream.write(compressed, 0, nbytes);
        stream.writeBytes(query.toString());
        stream.close();
    }

    public static int max(int[] vec) {
        int max = Integer.MIN_VALUE;

        for(int v : vec)
            if(v > max)
                max = v;

        return max;
    }

}