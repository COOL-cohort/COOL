package com.nus.cool.core.util.writer;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;


import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.writestore.ChunkWS;
import com.nus.cool.core.io.writestore.MetaChunkWS;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.IntegerUtil;

public class NativeDataWriter implements DataWriter {

    private TableSchema tableSchema;

    private final File outputDir;

    private final long chunkSize;

    private final long cubletSize;


    /**
     * states
     */
    private boolean initalized = false;

    private boolean finished = false;
    /**
     * below are variables describing the dataset building context
     */

    /**
     * initialzied once
     */
    private int userKeyIndex;
    
    private MetaChunkWS metaChunk;
    
    /**
     * updated as building progress
     */
    private int offset = 0;

    private int tupleCount = Integer.MAX_VALUE;

    private String lastUser = null;

    private List<Integer> chunkOffsets = Lists.newArrayList();

    private ChunkWS chunk;

    private DataOutputStream out = null;

    public NativeDataWriter(TableSchema schema, File outputDir, long chunkSize, long cubletSize) throws IOException {
        this.tableSchema = schema;
        this.outputDir = outputDir;
        this.chunkSize = chunkSize;
        this.cubletSize = cubletSize;
    }
    
    @Override
    public boolean Initialize() throws IOException {
        if (initalized) return true;
        this.userKeyIndex = tableSchema.getUserKeyField();
        // when there is no user key, using any field for the additional
        //  condition on chunk switch is ok.
        if (this.userKeyIndex == -1) this.userKeyIndex = 0; 
        // cublet
        this.offset = 0;
        this.metaChunk = MetaChunkWS.newMetaChunkWS(this.tableSchema, 0);
        this.out = newCublet();
        // chunk
        this.tupleCount = 0;
        this.chunk = ChunkWS.newChunk(this.tableSchema, this.metaChunk.getMetaFields(), this.offset);
        return true;
    }

    private void finishChunk() throws IOException {
        offset += chunk.writeTo(out);
        chunkOffsets.add(offset - Ints.BYTES);
    }
    
    private boolean maybeSwitchChunk(String curUser) throws IOException {
        if ((tupleCount < chunkSize) || (curUser == lastUser)) return false;
        finishChunk();
        chunk = ChunkWS.newChunk(tableSchema, metaChunk.getMetaFields(), offset);
        tupleCount = 0;
        return true;
    }

    private void finishCublet() throws IOException {
        offset += new MetaChunkWS(tableSchema, offset, metaChunk.getMetaFields()).writeTo(out);
        chunkOffsets.add(offset - Ints.BYTES);
        out.writeInt(IntegerUtil.toNativeByteOrder(chunkOffsets.size()));
        for (int chunkOff : chunkOffsets) {
          out.writeInt(IntegerUtil.toNativeByteOrder(chunkOff));
        }
        out.writeInt(IntegerUtil.toNativeByteOrder(offset));
        out.flush();
        out.close();
    }

    private DataOutputStream newCublet() throws IOException {
        String fileName = Long.toHexString(System.currentTimeMillis()) + ".dz";
        System.out.println("[*] A new cublet "+ fileName + " is created!");
        File cublet = new File(outputDir, fileName);
        DataOutputStream out = new DataOutputStream(new FileOutputStream(cublet));
        offset = 0;
        chunkOffsets.clear();
        return out;
    }

    private void maybeSwitchCublet() throws IOException {
        if (offset < cubletSize) return;
        finishCublet();
        out = newCublet();
    }

    @Override
    public boolean Add(Object tuple) throws IOException {
        if (!(tuple instanceof String[])) {
            System.out.println(
                "Unexpected tuple type: tuple not in valid type for DataWriter");
            return false;
        }
        String[] fields = (String[]) tuple;
        String curUser = fields[userKeyIndex];
        if (lastUser == null) lastUser = curUser;
        // start a new chunk
        if (maybeSwitchChunk(curUser)) maybeSwitchCublet();
        lastUser = curUser;
        // update metachunk / metafield
        metaChunk.update(fields);
        // update data chunk 
        chunk.put(fields);
        tupleCount++;
        return true;
    }

    @Override
    public void Finish() throws IOException {
        if (finished) return;
        finishChunk();
        finishCublet();
    }

    @Override
    public void close() throws IOException {
        if (!finished) Finish();
    }
}
