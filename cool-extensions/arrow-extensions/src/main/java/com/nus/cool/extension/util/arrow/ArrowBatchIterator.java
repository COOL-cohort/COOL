package com.nus.cool.extension.util.arrow;

import java.util.Iterator;

import org.apache.arrow.vector.VectorSchemaRoot;

public class ArrowBatchIterator implements Iterator<ArrowRowView> {
  private final VectorSchemaRoot root;
  private int index;

  public ArrowBatchIterator(VectorSchemaRoot root) {
    this.root = root;
    this.index = 0;
  }

  @Override
  public ArrowRowView next() {
    return new ArrowRowView(root, index++);
  }

  @Override
  public boolean hasNext() {
    return index < root.getRowCount();
  }
}
