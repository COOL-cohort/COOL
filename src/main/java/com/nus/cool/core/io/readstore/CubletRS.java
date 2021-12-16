package com.nus.cool.core.io.readstore;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.nus.cool.core.io.Input;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.schema.TableSchema;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Read cublet store
 * <p>
 * cublet layout
 * ---------------------------------------------------------------------------
 * | meta chunk | data chunk 1 | ... | data chunk n | header | header offset |
 * ---------------------------------------------------------------------------
 * <p>
 * header layout
 * ----------------------------------
 * | #chunks | chunk header offsets |
 * ----------------------------------
 * where
 * #chunks = number of chunk, including meta chunk
 * chunk header offsets record chunk's header offset position
 */
public class CubletRS implements Input {

    /**
     * MetaChunk for this cublet
     */
    @Getter
    private MetaChunkRS metaChunk;

    /**
     * BitSet list for query result
     */
    private List<BitSet> bitSets = Lists.newArrayList();

    @Getter
    private List<ChunkRS> dataChunks = Lists.newArrayList();

    private TableSchema schema;

    @Getter
    @Setter
    private String file;

    @Getter
    private int limit;

    public CubletRS(TableSchema schema) {
        this.schema = checkNotNull(schema);
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        // Read header offset
        int end = buffer.limit();
        this.limit = end;
        int headOffset;
        buffer.position(end - Ints.BYTES);
        int tag = buffer.getInt();
        if (tag != 0) {
            headOffset = tag;
        } else {
            buffer.position(end - Ints.BYTES - Ints.BYTES);
            int size = buffer.getInt();
            buffer.position(end - Ints.BYTES - Ints.BYTES - Ints.BYTES);
            end = buffer.getInt();
            buffer.position(end - Ints.BYTES);
            headOffset = buffer.getInt();
            buffer.position(end);
            for (; size > 0; size--) this.bitSets.add(SimpleBitSetCompressor.read(buffer));
        }

        // Get #chunk and chunk offsets
        buffer.position(headOffset);
        int chunks = buffer.getInt();
        int[] chunkOffsets = new int[chunks];
        for (int i = 0; i < chunks; i++)
            chunkOffsets[i] = buffer.getInt();

        // Read meta chunk
        this.metaChunk = new MetaChunkRS(this.schema);
        buffer.position(chunkOffsets[0]);
        int chunkHeadOffset = buffer.getInt();
        buffer.position(chunkHeadOffset);
        this.metaChunk.readFrom(buffer);

        for (int i = 1; i < chunks; i++) {
            ChunkRS chunk = new ChunkRS(this.schema);
            buffer.position(chunkOffsets[i]);
            chunkHeadOffset = buffer.getInt();
            buffer.position(chunkHeadOffset);
            chunk.readFrom(buffer);
            this.dataChunks.add(chunk);
        }
    }
}
