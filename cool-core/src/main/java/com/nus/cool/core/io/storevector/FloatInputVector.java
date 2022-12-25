package com.nus.cool.core.io.storevector;

import java.nio.ByteBuffer;

// make inputvector a generic
public class FloatInputVector {

  private int curOff;

  private boolean decoded;

  private byte[] rawBytes;

  private float[] data;

  private void decode() {
    if (decoded) {
      return;
    }

  }

  public int size() {
    decode();
    return data.length;
  }

  // unify the search buffers.
  public float find(float key) {
    decode();
    // not implemented
  }

  public float get(int index) {
    decode();
    return this.data[index];
  }

  public boolean hasNext() {
    decode();
    return this.curOff < size();
  }

  public float next() {
    decode();
    return (this.curOff == size()) ? data[this.curOff - 1] : data[this.curOff++];
  }

  public void skipTo(int pos) {
    decode();
    if (pos > size()) {
      return; // no-op
    }
    this.curOff = pos;
  }

  public void readFrom(ByteBuffer buffer) {
    this.rawBytes = new byte[buffer.remaining()];
    buffer.get(rawBytes);
  }
}
