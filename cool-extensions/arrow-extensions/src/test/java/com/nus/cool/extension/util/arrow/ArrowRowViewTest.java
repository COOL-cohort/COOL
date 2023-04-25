package com.nus.cool.extension.util.arrow;

import java.util.Arrays;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing Arrow tuple construction.
 */
public class ArrowRowViewTest {

  @Test(dataProvider = "ArrowRowReviewDP")
  public void testValid(VectorSchemaRoot root) {
    ArrowRowView arv1 = new ArrowRowView(root, 1);
    Assert.assertTrue(arv1.valid());
    ArrowRowView arv2 = new ArrowRowView(root, 4);
    Assert.assertFalse(arv2.valid());
  }

  @Test(dataProvider = "ArrowRowReviewDP")
  public void testGetField(VectorSchemaRoot root) {
    ArrowRowView arv = new ArrowRowView(root, 1);
    Assert.assertTrue(arv.getField("height").isPresent());
    Assert.assertEquals(arv.getField("height").get(), 20);
    Assert.assertTrue(arv.getField("name").isPresent());
    Assert.assertEquals(arv.getField("name").get().toString(), "Amie");
  }


  /**
   * Data provider for arrow tuple generation. 
   */
  @DataProvider(name = "ArrowRowReviewDP")
  public Object[][] dpArgs() {
    VectorSchemaRoot root;

    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("height", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Arrays.asList(name, age));
    BufferAllocator allocator = new RootAllocator();
    root = VectorSchemaRoot.create(schema, allocator);

    VarCharVector nameVector = (VarCharVector) root.getVector("name");
    nameVector.allocateNew(3);
    nameVector.set(0, "John".getBytes());
    nameVector.set(1, "Amie".getBytes());
    nameVector.set(2, "Helen".getBytes());
    nameVector.setValueCount(3);
    IntVector ageVector = (IntVector) root.getVector("height");
    ageVector.allocateNew(3);
    ageVector.set(0, 40);
    ageVector.set(1, 20);
    ageVector.set(2, 80);
    ageVector.setValueCount(3);
    root.setRowCount(3);
    return new Object[][]{
            {root}
    };
  }
}



