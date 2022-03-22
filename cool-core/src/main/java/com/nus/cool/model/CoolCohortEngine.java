package com.nus.cool.model;

import com.nus.cool.core.cohort.CohortUserSection;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.ExtendedCohortSelection;
import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.ZIntBitCompressor;
import com.nus.cool.core.io.readstore.*;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CoolCohortEngine {

    public List<String> listCohortUsers(CubeRS cube, List<Integer> inCohort){
        List<String> outCohort = new ArrayList<>();

        CubletRS cubletRS = cube.getCublets().get(0);
        MetaChunkRS metaChunk = cubletRS.getMetaChunk();

        TableSchema tableSchema = cube.getSchema();
        MetaFieldRS metaField = metaChunk.getMetaField(tableSchema.getUserKeyField(), FieldType.UserKey);
        for(Integer userID : inCohort){
            outCohort.add(metaField.getString(userID));
        }
        return outCohort;
    }

    public List<Integer> selectCohortUsers(CubeRS cube,
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

            userList.addAll((List<Integer>)gamma.getCubletResults());
        }

        return userList;
    }


    public void createCohort(ExtendedCohortQuery query, List<Integer> users, File cohortRoot) throws IOException {
        String cohortName = query.getOutputCohort();
        File cohort = new File(cohortRoot, cohortName);

        cohort.createNewFile();

        DataOutputStream stream = new DataOutputStream(new FileOutputStream(cohort));
        int[] userArray = new int[users.size()];
        Iterator<Integer> iter = users.iterator();
        for (int i = 0; i < userArray.length; i++) {
            userArray[i] = iter.next().intValue();
        }

        int maxValue = Integer.MIN_VALUE;
        for(int v : userArray)
            if(v > maxValue) maxValue = v;

        Compressor compressor = new ZIntBitCompressor(
                Histogram.builder()
                        .max(maxValue)
                        .numOfValues(userArray.length)
                        .uniqueValues(userArray.length)
                        .build());
        byte[] compressed = new byte[compressor.maxCompressedLength()];
        int nbytes = compressor.compress(userArray, 0, userArray.length,
                compressed, 0, compressed.length);
        stream.write(compressed, 0, nbytes);
        stream.writeBytes(query.toString());
        stream.close();
    }

}
