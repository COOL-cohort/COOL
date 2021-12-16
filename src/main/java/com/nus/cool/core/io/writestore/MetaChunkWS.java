package com.nus.cool.core.io.writestore;

import com.google.common.primitives.Ints;
import com.nus.cool.core.io.Output;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.ChunkType;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.IntegerUtil;
import lombok.Getter;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * MetaChunk write store
 * <p>
 * MetaChunk layout
 * ------------------------------------------
 * | MetaChunkData | header | header offset |
 * ------------------------------------------
 * <p>
 * MetaChunkData layout
 * -------------------------------------
 * | field 1 | field 2 | ... | field N |
 * -------------------------------------
 * <p>
 * header layout
 * ---------------------------------------
 * | ChunkType | #fields | field offsets |
 * ---------------------------------------
 * where
 * ChunkType = ChunkType.META
 * #fields = number of fields
 */
public class MetaChunkWS implements Output {

    private final int offset;

    @Getter
    private final MetaFieldWS[] metaFields;

    private final TableSchema schema;

    /**
     * Create a MetaChunkWS instance
     *
     * @param schema     TableSchema
     * @param offset     Offset in out stream
     * @param metaFields meta fields for this meta chunk
     */
    public MetaChunkWS(TableSchema schema, int offset, MetaFieldWS[] metaFields) {
        this.metaFields = checkNotNull(metaFields);
        this.schema = checkNotNull(schema);
        checkArgument(offset >= 0 && metaFields.length > 0);
        this.offset = offset;
    }

    public static MetaChunkWS newMetaChunkWS(TableSchema schema, int offset) {
        OutputCompressor compressor = new OutputCompressor();
        Charset charset = Charset.forName(schema.getCharset());
        int n = schema.getFields().size();
        MetaFieldWS[] metaFields = new MetaFieldWS[n];
        for (int i = 0; i < metaFields.length; i++) {
            FieldSchema fieldSchema = schema.getField(i);
            FieldType fieldType = fieldSchema.getFieldType();
            switch (fieldType) {
                case AppKey:
                case UserKey:
                case Action:
                case Segment:
                    metaFields[i] = new HashMetaFieldWS(fieldType, charset, compressor);
                    break;
                case ActionTime:
                case Metric:
                    metaFields[i] = new RangeMetaFieldWS(fieldType);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid field type: " + fieldType);
            }
        }
        return new MetaChunkWS(schema, offset, metaFields);
    }

    /**
     * Put a tuple into the meta chunk
     *
     * @param tuple Plain data line
     */
    public void put(String[] tuple) {
        checkNotNull(tuple);
        checkArgument(tuple.length == 2);
        this.metaFields[this.schema.getFieldID(tuple[0])].put(tuple[1]);
    }

    /**
     * Complete MetaFields
     */
    public void complete() {
        for (MetaFieldWS metaField : this.metaFields) {
            metaField.complete();
        }
    }

    /**
     * Write MetaChunkWS to out stream and return bytes written
     *
     * @param out stream can write data to output stream
     * @return bytes written
     * @throws IOException If an I/O error occurs
     */
    @Override
    public int writeTo(DataOutput out) throws IOException {
        int bytesWritten = 0;

        // Store field offsets and write MetaFields as MetaChunkData layout
        int[] offsets = new int[this.metaFields.length];
        for (int i = 0; i < this.metaFields.length; i++) {
            offsets[i] = this.offset + bytesWritten;
            bytesWritten += this.metaFields[i].writeTo(out);
        }

        // Store header offset for MetaChunk layout
        int headOffset = this.offset + bytesWritten;

        // Write ChunkType for header layout
        out.writeByte(ChunkType.META.ordinal());
        bytesWritten++;

        // Write fields for header layout
        out.writeInt(IntegerUtil.toNativeByteOrder(this.metaFields.length));
        bytesWritten++;

        // Write field offsets for header layout
        for (int offset : offsets) {
            out.writeInt(IntegerUtil.toNativeByteOrder(offset));
            bytesWritten += Ints.BYTES;
        }

        // Write header offset for MetaChunk layout
        out.writeInt(IntegerUtil.toNativeByteOrder(headOffset));
        bytesWritten += Ints.BYTES;
        return bytesWritten;
    }
}
