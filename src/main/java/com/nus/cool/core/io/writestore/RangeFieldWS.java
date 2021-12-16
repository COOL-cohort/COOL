package com.nus.cool.core.io.writestore;

import com.google.common.primitives.Ints;
import com.nus.cool.core.io.DataInputBuffer;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.schema.Codec;
import com.nus.cool.core.schema.CompressType;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.util.ArrayUtil;
import com.nus.cool.core.util.IntegerUtil;
import com.nus.cool.core.util.converter.DayIntConverter;

import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Range index, used to store chunk data for two fieldTypes, including
 * ActionTime, Metric.
 * <p>
 * Data layout
 * ------------------------------
 * | codec | min | max | values |
 * ------------------------------
 * where
 * min = min of the values
 * max = max of the values
 * values = column data (compressed)
 */
public class RangeFieldWS implements FieldWS {

    /**
     * Field index to get data from tuple
     */
    private int i;

    private FieldType fieldType;

    private DataOutputBuffer buffer = new DataOutputBuffer();

    private OutputCompressor compressor;

    public RangeFieldWS(FieldType fieldType, int i, OutputCompressor compressor) {
        checkArgument(i >= 0);
        this.i = i;
        this.fieldType = fieldType;
        this.compressor = checkNotNull(compressor);
    }

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public void put(String[] tuple) throws IOException {
        if (this.fieldType == FieldType.ActionTime) {
            DayIntConverter converter = new DayIntConverter();
            this.buffer.writeInt(converter.toInt(tuple[this.i]));
        } else
            this.buffer.writeInt(Integer.parseInt(tuple[i]));
    }

    @Override
    public int writeTo(DataOutput out) throws IOException {
        int bytesWritten = 0;
        int[] key = new int[2];
        int[] value = new int[this.buffer.size() / Ints.BYTES];

        // Read column data
        // TODO: Bloated code
        try (DataInputBuffer input = new DataInputBuffer()) {
            input.reset(this.buffer);
            for (int i = 0; i < value.length; i++)
                value[i] = input.readInt();
        }

        key[0] = ArrayUtil.min(value);
        key[1] = ArrayUtil.max(value);

        // Write codec
        out.write(Codec.Range.ordinal());
        bytesWritten++;
        // Write min value
        out.writeInt(IntegerUtil.toNativeByteOrder(key[0]));
        bytesWritten += Ints.BYTES;
        // Write max value
        out.writeInt(IntegerUtil.toNativeByteOrder(key[1]));
        bytesWritten += Ints.BYTES;

        // Write values, i.e. the data within the column
        int count = value.length;
        int rawSize = count * Ints.BYTES;
        Histogram hist = Histogram.builder()
                .min(key[0])
                .max(key[1])
                .numOfValues(count)
                .rawSize(rawSize)
                .type(CompressType.ValueFast)
                .build();
        this.compressor.reset(hist, value, 0, value.length);
        bytesWritten += this.compressor.writeTo(out);
        return bytesWritten;
    }
}
