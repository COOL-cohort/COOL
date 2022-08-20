package com.nus.cool.core.cohort.refactor.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.primitives.Ints;
import com.nus.cool.core.io.Input;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.ZIntBitInputVector;

/**
 * rewrite of CohortRS for the updated persistent cohort format
 */
public class CohortRS implements Input {

  boolean initialized;

  private InputVector[] usersByCublet;

  private boolean[] loaded;

  private int[] offsets;

  private ByteBuffer buffer;

  public CohortRS() {
    initialized = false;
  }

  private void loadCubletUsers(int cubletIdx) {
    buffer.position(offsets[cubletIdx]);
    usersByCublet[cubletIdx] = ZIntBitInputVector.load(buffer);
    loaded[cubletIdx] = true;
  }

  public InputVector getUsers(int cubletIdx)
    throws IllegalStateException, IllegalArgumentException {
    if (!initialized) throw new IllegalStateException();
    if (cubletIdx < offsets.length - 1) throw new IllegalArgumentException();
    // already loaded
    if (loaded[cubletIdx]) {
      return usersByCublet[cubletIdx];
    }
    // lazy loading
    loadCubletUsers(cubletIdx);
    return usersByCublet[cubletIdx];
  }
  
  @Override
  public void readFrom(ByteBuffer buffer) {
    this.buffer = buffer;
    this.buffer.position(this.buffer.limit() - Ints.BYTES);
    int headerOffset = buffer.getInt();
    int numCublets = (buffer.limit()-Ints.BYTES - headerOffset) / Ints.BYTES;
    // initialize attributes
    offsets = new int[numCublets];
    usersByCublet = new InputVector[numCublets];
    loaded = new boolean[numCublets];
    Arrays.fill(loaded, Boolean.FALSE);
    // load offsets
    this.buffer.position(headerOffset);
    for (int i = 0; i < numCublets; i++) {
      offsets[i] = buffer.getInt();
    }
    initialized = true;
  }
}
