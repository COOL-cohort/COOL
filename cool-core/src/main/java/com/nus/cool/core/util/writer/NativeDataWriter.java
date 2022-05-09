package com.nus.cool.core.util.writer;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;


import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.writestore.DataChunkWS;
import com.nus.cool.core.io.writestore.MetaChunkWS;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.IntegerUtil;
import lombok.RequiredArgsConstructor;

import javax.validation.constraints.NotNull;

@RequiredArgsConstructor
public class NativeDataWriter implements DataWriter {

    @NotNull
    private final TableSchema tableSchema;

    @NotNull
    private final File outputDir;

    @NotNull
    private final long chunkSize;

    @NotNull
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

    // record the Header offset of each chunk
    private final List<Integer> chunkHeaderOffsets = Lists.newArrayList();

    private DataChunkWS dataChunk;

    private DataOutputStream out = null;

    @Override
    public boolean Initialize() throws IOException {
        if (initalized) return true;
        this.userKeyIndex = tableSchema.getUserKeyField();
        // when there is no user key, using any field for the additional condition on chunk switch is ok.
        if (this.userKeyIndex == -1) this.userKeyIndex = 0; 
        // cublet
        // create metaChunk instance, default offset to be 0, update offset when write later.
        this.metaChunk = MetaChunkWS.newMetaChunkWS(this.tableSchema, 0);
        this.out = newCublet();
        // chunk
        this.tupleCount = 0;
        this.offset = 0;
        // create dataChunk instance
        this.dataChunk = DataChunkWS.newDataChunk(this.tableSchema, this.metaChunk.getMetaFields(), this.offset);
        this.initalized = true;
        return true;
    }

    /**
     * Update current offset, begin write to a new dataChunk
     * @throws IOException
     */
    private void finishChunk() throws IOException {
        offset += dataChunk.writeTo(out);
        // record the header offset in chunkHeaderOffsets.
        chunkHeaderOffsets.add(offset - Ints.BYTES);
    }

    /**
     * If chunkSize is greater 65536 or meet last user
     * @param curUser current user id
     * @return is chunk full or is last user
     * @throws IOException
     */
    private boolean maybeSwitchChunk(String curUser) throws IOException {
        if ((tupleCount < chunkSize) || (curUser.equals(lastUser))) return false;
        finishChunk();
        // create a new data chunk, init tuple Count
        dataChunk = DataChunkWS.newDataChunk(tableSchema, metaChunk.getMetaFields(), offset);
        tupleCount = 0;
        return true;
    }

    /**
     * Write metaChunk lastly
     * @throws IOException
     */
    private void finishCublet() throws IOException {
        // write metaChunk begin from current offset.
        this.metaChunk.updateBeginOffset(this.offset);
        offset += this.metaChunk.writeTo(out);
        // record header offset
        chunkHeaderOffsets.add(offset - Ints.BYTES);
        // 1. write number of chunks
        out.writeInt(IntegerUtil.toNativeByteOrder(chunkHeaderOffsets.size()));
        // 2. write header of each chunk
        for (int chunkOff : chunkHeaderOffsets) {
          out.writeInt(IntegerUtil.toNativeByteOrder(chunkOff));
        }
        // 3. write the header offset.
        out.writeInt(IntegerUtil.toNativeByteOrder(offset));
        // 4. flush after writing whole Cublet.
        out.flush();
        out.close();
    }

    /**
     * Create I/O stream to write data.
     * @return DataOutputStream
     * @throws IOException
     */
    private DataOutputStream newCublet() throws IOException {
        String fileName = Long.toHexString(System.currentTimeMillis()) + ".dz";
        System.out.println("[*] A new cublet "+ fileName + " is created!");
        File cublet = new File(outputDir, fileName);
        DataOutputStream out = new DataOutputStream(new FileOutputStream(cublet));
        offset = 0;
        chunkHeaderOffsets.clear();
        return out;
    }

    /**
     * Switch a new cublet File once meet 1GB
     * @throws IOException
     */
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
        String[] insertTuple = (String[]) tuple;
        String curUser = insertTuple[userKeyIndex];
        if (lastUser == null) lastUser = curUser;
        // start a new chunk
        if (maybeSwitchChunk(curUser)) maybeSwitchCublet();
        lastUser = curUser;
        // update metachunk / metafield
        metaChunk.put(insertTuple);
        // update data chunk 
        dataChunk.put(insertTuple);
        tupleCount++;
        return true;
    }

    @Override
    public void Finish() throws IOException {
        if (finished) return;
        finishChunk();
        finishCublet();
        finished = true;
    }

    @Override
    public void close() throws IOException {
        // force close the out stream
        if (!finished) Finish();
    }
}
