package com.nus.cool.core.util.writer;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.StringHashField;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Testing ListDataWriter.
 */
public class ListDataWriterTest {

  @Test
  public void testListDataWriter() {
    FieldValue[] input1 = { new StringHashField("s11"), new StringHashField("s12") };
    FieldValue[] input2 = { new StringHashField("s21"), new StringHashField("s22") };
    List<String> out = new ArrayList<>();
    ListDataWriter writer = new ListDataWriter(out);
    try {
      writer.initialize();
      writer.add(input1);
      writer.add(input2);
      writer.finish();
      Assert.assertEquals(out.get(0), Arrays.toString(input1));
      Assert.assertEquals(out.get(1), Arrays.toString(input2));
      writer.close();
    } catch (IOException e) {
      System.err.println(e);
    }
  }
}
