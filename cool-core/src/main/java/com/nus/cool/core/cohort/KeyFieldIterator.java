package com.nus.cool.core.cohort;

import java.util.Optional;

import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.RLEInputVector;

import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
public class KeyFieldIterator {
  private final RLEInputVector input;

  private final InputVector keys;

  private RLEInputVector.Block block;

  public boolean next() {
    if (!input.hasNext()) return false;
    input.nextBlock(block);
    return true;
  }

  // return the id of the next ke
  public int key() {
    return keys.get(block.value);
  }

  public int getStartOffset() {
    return block.off;
  }

  public int getEndOffset() {
    return block.off + block.len;
  }

  
  @AllArgsConstructor
  public static class Builder {
    @NonNull
    private final FieldRS keyField;

    public Optional<KeyFieldIterator> build() {
      return Optional.ofNullable(
        (keyField.getValueVector() instanceof RLEInputVector)
        ? new KeyFieldIterator(
          (RLEInputVector) keyField.getValueVector(), keyField.getKeyVector(), new RLEInputVector.Block())
        : null
      );
    }
  }
}
