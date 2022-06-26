package com.nus.cool.extension.util.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.List;
import static java.util.Arrays.asList;

public class ArrowRowViewTest {
    private static VectorSchemaRoot root;
    @BeforeMethod
    public void setUp() {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("height", FieldType.nullable(new ArrowType.Int(32, true)), null);
        FieldType intType = new FieldType(true, new ArrowType.Int(32, true), null);
        Schema schema = new Schema(asList(name, age));
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
//        System.out.print("here1="+root.contentToTSVString());

    }

    @Test
    public void testValid() {
        ArrowRowView arv1 = new ArrowRowView(root,1);
        Assert.assertTrue(arv1.valid());
        ArrowRowView arv2 = new ArrowRowView(root,4);
        Assert.assertFalse(arv2.valid());
    }

    @Test
    public void testGetField() {
        {
            ArrowRowView arv = new ArrowRowView(root,1);
            Assert.assertTrue(arv.getField("height").isPresent());
            Assert.assertEquals(arv.getField("height").get(), 20);
            Assert.assertTrue(arv.getField("name").isPresent());
            Assert.assertEquals(arv.getField("name").get().toString(), "Amie");

        }
    }

}