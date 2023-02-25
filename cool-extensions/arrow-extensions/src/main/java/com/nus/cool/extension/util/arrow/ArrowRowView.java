package com.nus.cool.extension.util.arrow;

import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Encapsulation of a record in an Arrow record batch.
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

  public static <T> T newVector(Class<T> c, String name, ArrowType type,
      BufferAllocator allocator) {
    return c.cast(FieldType.nullable(type).createNewSingleVector(name, allocator, null));
  }
}
