package com.nus.cool.core.io.storevector;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.compression.BitVectorCompressor;
import com.nus.cool.core.io.compression.Compressor;
import com.nus.cool.core.io.compression.CompressorOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Testing BitVector input vector.
 */
public class BitVectorInputVectorTest {

  @Test
  public void test() {
    List<FieldValue> numbers = Arrays.asList(0, 1, 2, 3)
        .stream()
        .map(x -> ValueWrapper.of(x))
        .collect(Collectors.toList());
    Compressor compressor = new BitVectorCompressor(ValueWrapper.of(3));
    // compress the bytes
    CompressorOutput compressed = compressor.compress(numbers);

    // load the bytes with
    ByteBuffer buffer = ByteBuffer.wrap(compressed.getBuf(), 0, compressed.getLen());
    InputVector<Integer> in = new BitVectorInputVector();
    in.readFrom(buffer);

    for (int i = 0; i < in.size(); i++) {
      Assert.assertEquals(in.get(i).intValue(), i);
    }
    for (int i = 0; i < in.size(); i++) {
      Assert.assertEquals(in.find(i).intValue(), i);
    }
  }
}
