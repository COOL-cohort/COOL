package com.nus.cool.extension.util.arrow;

import java.util.Optional;

import org.apache.arrow.vector.VectorSchemaRoot;

import lombok.AllArgsConstructor;

/**
 * Encapsulation of a record in an Arrow record batch
 */
@AllArgsConstructor
public class ArrowRowView {
  private final VectorSchemaRoot root;
  private final int index;

  public boolean valid() {
    return index < root.getRowCount();
  }

  public Optional<Object> getField(String name) {
    return Optional.ofNullable( valid() ? root.getVector(name) : null)
                   .map(x -> x.getObject(index));
  }
}
