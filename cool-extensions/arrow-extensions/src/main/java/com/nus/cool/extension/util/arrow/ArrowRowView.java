package com.nus.cool.extension.util.arrow;

import java.util.Optional;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.BitVector;

import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.*;
import java.util.Collections;
import static java.util.Arrays.asList;
import org.apache.arrow.memory.*;


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
    return Optional.ofNullable(valid() ? root.getVector(name) : null)
            .map(x -> x.getObject(index));
  }

  public static <T> T newVector(Class<T> c, String name, ArrowType type, BufferAllocator allocator) {
    return c.cast(FieldType.nullable(type).createNewSingleVector(name, allocator, null));
  }
}