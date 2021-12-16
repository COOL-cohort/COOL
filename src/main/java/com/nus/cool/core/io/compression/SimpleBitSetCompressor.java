package com.nus.cool.core.io.compression;

import com.google.common.primitives.Ints;
import com.nus.cool.core.util.IntegerUtil;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Compress BitSet using rle schema, prefer sorted BitSet.
 *
 * Data layout
 * ------------------------------------
 * | sign | blk1 | blk2 | ... | bln n |
 * ------------------------------------
 * where
 * sign = bit, first of BitSet
 * blk = number of bits for same bit
 */
public class SimpleBitSetCompressor {

    public static int compress(BitSet bs, DataOutput out) throws IOException {
        int pos1 = 0, pos2, bytesWritten = 0;
        List<Integer> blks = new ArrayList<>();
        boolean sign = bs.get(pos1);
        while (true) {
            if (sign) {
                pos2 = bs.nextClearBit(pos1);
            } else {
                pos2 = bs.nextSetBit(pos1);
            }
            if (pos2 < 0 & !sign) {
                break;
            }
            blks.add(pos2 - pos1);
            pos1 = pos2;
            sign = !sign;
        }
        out.write(bs.get(0) ? 1 : 0);
        bytesWritten++;
        out.writeInt(IntegerUtil.toNativeByteOrder(blks.size()));
        bytesWritten += Ints.BYTES;
        for (int blk : blks) {
            out.writeInt(IntegerUtil.toNativeByteOrder(blk));
            bytesWritten += Ints.BYTES;
        }
        return bytesWritten;
    }

    public static BitSet read(ByteBuffer buff) {
        boolean sign = buff.get() != 0;
        int blks = buff.getInt();
        BitSet bs = new BitSet();
        int pos = 0, len;
        for (int i = 0; i < blks; i++) {
            len = buff.getInt();
            if (sign) {
                bs.set(pos, pos + len);
            }
            pos = pos + len;
            sign = !sign;
        }
        return bs;
    }

}
