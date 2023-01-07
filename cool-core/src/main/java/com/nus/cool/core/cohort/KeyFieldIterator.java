// package com.nus.cool.core.cohort;

// import com.nus.cool.core.io.readstore.FieldRS;
// import com.nus.cool.core.io.storevector.InputVector;
// import com.nus.cool.core.io.storevector.RLEInputVector;
// import java.util.Optional;
// import lombok.AllArgsConstructor;
// import lombok.NonNull;

// [TODO] need logic update

// /**
//  * Iterator class of a key field. (used for UserKey)
//  */
// @AllArgsConstructor
// public class KeyFieldIterator {
//   private final RLEInputVector input;

//   private final InputVector keys;

//   private RLEInputVector.Block block;

//   /**
//    * Iterate to the next item.
//    */
//   public boolean next() {
//     if (!input.hasNext()) {
//       return false;
//     }
//     input.nextBlock(block);
//     return true;
//   }

//   // return the id of the next key
//   public int key() {
//     return keys.get(block.value);
//   }

//   public int getStartOffset() {
//     return block.off;
//   }

//   public int getEndOffset() {
//     return block.off + block.len;
//   }

//   /**
//    * Builder class of KeyFieldIterator.
//    */
//   @AllArgsConstructor
//   public static class Builder {
//     @NonNull
//     private final FieldRS keyField;

//     /**
//      * Build and iterator over key field.
//      */
//     public Optional<KeyFieldIterator> build() {
//       return Optional.ofNullable((keyField.getValueVector() instanceof RLEInputVector)
//           ? new KeyFieldIterator((RLEInputVector) keyField.getValueVector(),
//               keyField.getKeyVector(), new RLEInputVector.Block())
//           : null);
//     }
//   }
// }
