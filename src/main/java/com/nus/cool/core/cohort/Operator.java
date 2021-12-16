package com.nus.cool.core.cohort;

import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.schema.TableSchema;

public interface Operator extends Cloneable {

    void init(TableSchema schema, CohortQuery query);

    void process(MetaChunkRS metaChunk);

    void process(ChunkRS chunk);
}
