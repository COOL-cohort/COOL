package com.nus.cool.core.io.storevector;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Input vector of a sequence of float.
 */
public class FloatInputVector implements InputVector<Float> {

  private int curOff;

  private boolean decoded;

  private ByteBuffer bitBuffer;

  private float[] data;

  private int floatCount;
  private int numBytes;

  private void decode() {
    if (decoded) {
      return;
    }
    data = new float[floatCount];
    // decode the encoded float bytes
    float lastVal = bitBuffer.getFloat();
    data[0] = lastVal;
    BitSet bs = BitSet.valueOf(bitBuffer);
    // decode each value
    int startoff = 0;
    int lastDiffStart = 0;
    int lastDiffSize = 32;
    BitSet xor = new BitSet(32);
    for (int i = 1; i < floatCount; i++) {
      if (!bs.get(startoff)) {
        // same as last value, stored a 0 bit.
        data[i] = lastVal;
        startoff++;
        continue;
      } else if (!bs.get(startoff + 1)) {
        // case 2: same xor size
        xor.clear();
        for (int j = 0; j < lastDiffSize; j++) {
          xor.set(lastDiffStart + j, bs.get(startoff + 2 + j));
        }
        xor.xor(BitSet.valueOf(new long[]{Integer.toUnsignedLong(Float.floatToIntBits(lastVal))}));
        float targetVal = Float.intBitsToFloat(xor.isEmpty() ? 0 : (int) xor.toLongArray()[0]);
        lastVal = targetVal;
        data[i] = lastVal;
        startoff += 2 + lastDiffSize;
      } else {
        // case 3:
        xor.clear();
        // get the leading 0 len
        for (int j = 0; j < 5; j++) {
          xor.set(j, bs.get(startoff + 2 + j));
        }
        lastDiffStart = xor.isEmpty() ? 0 : (int) xor.toLongArray()[0];
        xor.clear();
        for (int j = 0; j < 6; j++) {
          xor.set(j, bs.get(startoff + 2 + 5 + j));
        }
        lastDiffSize = xor.isEmpty() ? 0 : (int) xor.toLongArray()[0];
        xor.clear();
        for (int j = 0; j < lastDiffSize; j++) {
          xor.set(lastDiffStart + j, bs.get(startoff + 2 + 5 + 6 + j));
        }
        xor.xor(BitSet.valueOf(new long[]{Integer.toUnsignedLong(Float.floatToIntBits(lastVal))}));
        float targetVal = Float.intBitsToFloat(xor.isEmpty() ? 0 : (int) xor.toLongArray()[0]);
        lastVal = targetVal;
        data[i] = lastVal;
        startoff += 2 + 5 + 6 + lastDiffSize; 
      }
    }
    decoded = true;
  }

  @Override
  public int size() {
    decode();
    return data.length;
  }

  @Override
  // unify the search buffers.
  public Float find(Float key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Float get(int index) {
    decode();
    return this.data[index];
  }

  @Override
  public boolean hasNext() {
    decode();
    return this.curOff < size();
  }

  @Override
  public Float next() {
    decode();
    return (this.curOff == size()) ? data[this.curOff - 1] : data[this.curOff++];
  }

  @Override
  public void skipTo(int pos) {
    decode();
    if (pos > size()) {
      return; // no-op
    }
    this.curOff = pos;
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    floatCount = buffer.getInt();
    numBytes = buffer.getInt();
    bitBuffer = buffer.slice();
    bitBuffer.limit(numBytes);
  }
}
