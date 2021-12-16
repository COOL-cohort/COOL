package com.nus.cool.loader;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.writestore.MetaChunkWS;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.parser.CsvTupleParser;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.LineTupleReader;
import com.nus.cool.core.util.reader.TupleReader;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class LocalLoader {

    private static int offset = 0;

    private static List<Integer> chunkOffsets = Lists.newArrayList();

    public static void load(TableSchema tableSchema, File dimensionFile, File dataFile, File outputDir, int chunkSize) throws IOException {
        TupleParser parser = new CsvTupleParser();
        MetaChunkWS metaChunk = newMetaChunk(dimensionFile, tableSchema, parser);
        DataOutputStream out = newCublet(outputDir, metaChunk);
        out.close();

    }

    private static MetaChunkWS newMetaChunk(File inputMetaFile, TableSchema schema, TupleParser parser) throws IOException {
        MetaChunkWS metaChunk = MetaChunkWS.newMetaChunkWS(schema, offset);
        try (TupleReader reader = new LineTupleReader(inputMetaFile)) {
            while (reader.hasNext()) {
                metaChunk.put(parser.parse(reader.next()));
            }
        }
        metaChunk.complete();
        return metaChunk;
    }

    private static DataOutputStream newCublet(File dir, MetaChunkWS metaChunk) throws IOException {
        File cublet = new File(dir, Long.toHexString(System.currentTimeMillis()) + ".dz");
        DataOutputStream out = new DataOutputStream(new FileOutputStream(cublet));
        offset = metaChunk.writeTo(out);
        chunkOffsets.clear();
        chunkOffsets.add(offset - Ints.BYTES);
        return out;
    }

}
