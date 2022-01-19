package com.nus.cool.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.*;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.io.readstore.*;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.IntegerUtil;
import com.nus.cool.core.util.converter.DayIntConverter;
import com.nus.cool.core.util.converter.NumericConverter;

import java.io.*;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class ExtendedCohortLoader {

    /**
     * Local model for cool
     */
    private static CoolModel coolModel;

    public static void main(String[] args) throws IOException {
        coolModel = new CoolModel(args[0]);

        // load cube
        coolModel.reload(args[1]);

        // load query
        ObjectMapper mapper = new ObjectMapper();
        ExtendedCohortQuery query = mapper.readValue(new File("health/query0.json"), ExtendedCohortQuery.class);

        System.out.println(query);

        List<ResultTuple> resultTuples = executeQuery(coolModel.getCube(query.getDataSource()), query);
        QueryResult result = QueryResult.ok(resultTuples);
        System.out.println(result.toString());
        coolModel.close();
    }

    /**
     * ececute quety
     *
     * @param cube the cube that stores the data we need
     * @param query the cohort query needed to process
     * @return the result of the query
     */
    public static List<ResultTuple> executeQuery(CubeRS cube, ExtendedCohortQuery query) throws IOException {
        List<CubletRS> cublets = cube.getCublets();
        TableSchema schema = cube.getSchema();
        List<ResultTuple> resultSet = Lists.newArrayList();

        List<BitSet> bitSets = Lists.newArrayList();
        // process each cublet
        for (CubletRS cublet : cublets) {
            MetaChunkRS metaChunk = cublet.getMetaChunk();
            CohortSelection sigma = new CohortSelection();
            CohortAggregation gamma = new CohortAggregation(sigma);
            gamma.init(schema, query);
            gamma.process(metaChunk);
            if (sigma.isBUserActiveCublet()) {
                List<ChunkRS> dataChunks = cublet.getDataChunks();
                for (ChunkRS dataChunk : dataChunks) {
                    gamma.process(dataChunk);
                    bitSets.add(gamma.getBs());
                }
            }
            if (tag) {
                int end = cublet.getLimit();
                DataOutputStream out = map.get(cublet.getFile());
                for (BitSet bs : bitSets) {
                    SimpleBitSetCompressor.compress(bs, out);
                }
                out.writeInt(IntegerUtil.toNativeByteOrder(end));
                out.writeInt(IntegerUtil.toNativeByteOrder(bitSets.size()));
                out.writeInt(IntegerUtil.toNativeByteOrder(0));
            }

            String cohortField = query.getCohortFields()[0];
            String actionTimeField = schema.getActionTimeFieldName();
            NumericConverter converter =
                    cohortField.equals(actionTimeField) ? new DayIntConverter() : null;
            MetaFieldRS cohortMetaField = metaChunk.getMetaField(cohortField);
            Map<CohortKey, Long> results = gamma.getCubletResults();
            for (Map.Entry<CohortKey, Long> entry : results.entrySet()) {
                CohortKey key = entry.getKey();
                int cId = key.getCohort();
                String cohort = converter == null ? cohortMetaField.getString(key.getCohort())
                        : converter.getString(cId);
                int age = key.getAge();
                long measure = entry.getValue();
                resultSet.add(new ResultTuple(cohort, age, measure));
            }
        }
        // merge the partial query results
        return ResultTuple.merge(resultSet);
    }
}
