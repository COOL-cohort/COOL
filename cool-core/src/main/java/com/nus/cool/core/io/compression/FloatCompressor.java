package com.nus.cool.core.io.compression;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

/**
 * Only accept float (32 bit).
 */
public class FloatCompressor implements Compressor {
  public static final int HEADACC = 4 + 4;

  public static final int maxWidth = 2 + 5 + 6 + 32;

  static class Context {
    Context(float lastVal) {
      this.lastVal = lastVal;
      this.lastDiffStart = 0;
      this.lastDiffSize = 0;
    }

    float lastVal;
    int lastDiffStart;
    int lastDiffSize;
  }

  private static void append(BitSet src, int srcStart, BitSet dest, int destStart, int size) {
    // simplest is to set bit by bit
    for (int i = 0; i < size; i++) {
      dest.set(destStart + i, src.get(srcStart + i));
    }
  }

  // return end offset.
  private static int add(BitSet dest, int startOff, float f, Context ctx) {
    // case 1: same value use 1 bit 0 to denote.
    if (f == ctx.lastVal) {
      return 1;
    }
    // get the xor value
    int xor = Float.floatToIntBits(f) ^ Float.floatToIntBits(ctx.lastVal);
    BitSet xorBits = BitSet.valueOf(new long[]{Integer.toUnsignedLong(xor)});
    // get the diff range
    int diffFirst = xorBits.nextSetBit(0);
    int diffLast = xorBits.previousSetBit(xorBits.size());
    int diffSize = diffLast - diffFirst + 1;

    ctx.lastVal = f;

    dest.set(startOff);
    if ((diffSize == ctx.lastDiffSize) && (diffFirst == ctx.lastDiffStart)) {
      // case 2: same xor size
      // set meaningful xor bits
      append(xorBits, diffFirst, dest, startOff + 2, diffSize);
      return 2 + diffSize;
    } else {
      // case 3: different xor size
      dest.set(startOff + 1);
      // set leading 0 len
      append(BitSet.valueOf(new long[]{diffFirst}), 0, dest,
          startOff + 2, 5);
      // set meaningful xor bits len
      append(BitSet.valueOf(new long[]{diffSize}), 0, dest,
          startOff + 2 + 5, 6);
      // set meaningful xor bits    
      append(xorBits, diffFirst, dest, startOff + 2 + 5 + 6, diffSize);
      ctx.lastDiffStart = diffFirst;
      ctx.lastDiffSize = diffSize;
      return 2 + 5 + 6 + diffSize;
    }
  }

  /**
   * Compress a buffer of floats. [todo] allow no-compression if saving is not huge.
   */
  public static byte[] compress(ByteBuffer src) {
    int floatCount = src.capacity() / 4;

    // generate header and put the first float
    ByteBuffer header = ByteBuffer.allocate(HEADACC);
    header.putInt(floatCount);
    int numBytePlaceHolder = 0;
    header.putInt(numBytePlaceHolder);
    final float firstVal = src.getFloat();
    header.putFloat(firstVal);
    header.position(0);
    // create a separate bitset for compression
    BitSet bs = BitSet.valueOf(header);

    Context ctx = new Context(firstVal);
    int curOff = (HEADACC + 4) * 8; // the first value
    while (--floatCount > 0) {
      curOff += add(bs, curOff, src.getFloat(), ctx);
    }
    byte[] ret = bs.toByteArray();
    header = ByteBuffer.wrap(ret);
    header.position(4);
    header.putInt(ret.length - HEADACC);
    return ret;
  }

  @Override
  public CompressorOutput compress(List<? extends FieldValue> src) {
    int floatCount = src.size();
    // generate header and put the first float
    ByteBuffer header = ByteBuffer.allocate(HEADACC + 4);
    header.putInt(floatCount);
    int numBytePlaceHolder = 0;
    header.putInt(numBytePlaceHolder);
    Iterator<? extends FieldValue> itr = src.iterator();
    final float firstVal = ((RangeField) itr.next()).getFloat();
    header.putFloat(firstVal);
    header.position(0);
    BitSet bs = BitSet.valueOf(header);

    Context ctx = new Context(firstVal);
    int curOff = (HEADACC + 4) * 8;
    while (itr.hasNext()) {
      curOff += add(bs, curOff, ((RangeField) itr.next()).getFloat(), ctx);
    }
    byte[] ret = bs.toByteArray();
    header = ByteBuffer.wrap(ret);
    header.position(4);
    header.putInt(ret.length - HEADACC);
    return new CompressorOutput(ret, ret.length);
  }
}
