package com.nus.cool.core.io.readstore;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.Input;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.InputVectorFactory;
import com.nus.cool.core.schema.ChunkType;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Data;

/**
 * CubeMeta describes the possible values of each field in a cube.
 */
public class CubeMetaRS implements Input {

  /**
   * TableSchema for this meta chunk.
   */
  private TableSchema schema;

  /**
   * Charset defined in table schema.
   */
  private Charset charset;

  /**
   * Stored data byte buffer.
   */
  private ByteBuffer buffer;

  /**
   * Offsets for fields in this meta chunk.
   */
  private int[] fieldOffsets;

  private interface FieldMeta extends Input {
    String generateJson();
  }

  @Data
  private class RangeFieldMeta implements FieldMeta {
    private FieldType type;

    private RangeField min;

    private RangeField max;

    public RangeFieldMeta(FieldType type) {
      this.type = type;
    }

    @Override
    public void readFrom(ByteBuffer buf) {
      if (this.type == FieldType.Float) {
        this.min = ValueWrapper.of(buffer.getFloat());
        this.max = ValueWrapper.of(buffer.getFloat());
      } else {
        // integer / action time
        this.min = ValueWrapper.of(buffer.getInt());
        this.max = ValueWrapper.of(buffer.getInt());
      }
    }

    @Override
    public String generateJson() {
      ObjectMapper mapper = new ObjectMapper();
      try {
        return mapper.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        System.err.println(e);
        return "{\"error\": \"failed to generate json\"}";
      }
    }
  }

  @Data
  private class HashFieldMeta implements FieldMeta {

    private Charset charset;

    private FieldType type;

    private List<String> values;

    public HashFieldMeta(FieldType type, Charset charset) {
      this.type = type;
      this.charset = charset;
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
      InputVector<String> valueVec;
      try {
        valueVec = InputVectorFactory.genStrFieldInputVector(buffer, charset);
      } catch (IllegalArgumentException e) {
        System.err.println(e);
        return;
      }

      int valueCount = valueVec.size();
      values = new ArrayList<>(valueCount);
      for (int i = 0; i < valueCount; i++) {
        String value = valueVec.get(i);
        values.add(value);
      }
    }

    @Override
    public String generateJson() {
      ObjectMapper mapper = new ObjectMapper();
      try {
        return mapper.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        System.err.println(e);
        return "{\"error\": \"failed to generate json\"}";
      }
    }
  }

  private Map<Integer, FieldMeta> fields = Maps.newHashMap();

  public CubeMetaRS(TableSchema schema) {
    this.schema = checkNotNull(schema);
    this.charset = Charset.forName(this.schema.getCharset());
  }

  @Override
  public void readFrom(ByteBuffer buffer) {
    // Read header
    // Get chunk type
    this.buffer = checkNotNull(buffer);
    buffer.position(buffer.limit() - Ints.BYTES);
    int headOffset = this.buffer.getInt();
    buffer.position(headOffset);
    ChunkType chunkType = ChunkType.fromInteger(this.buffer.get());
    if (chunkType != ChunkType.META) {
      throw new IllegalStateException("Expect MetaChunk, but reads: " + chunkType);
    }

    // Get #fields
    int numFields = this.buffer.getInt();
    // Get field offsets
    this.fieldOffsets = new int[numFields];
    for (int i = 0; i < numFields; i++) {
      fieldOffsets[i] = this.buffer.getInt();
    }
    // Fields are loaded only if they are called
  }

  /**
   * Return a json string that describes the possible values of a field.
   */
  public synchronized String getFieldMeta(String fieldName) {
    int id = this.schema.getFieldID(fieldName);
    if (id < 0 || id >= this.fieldOffsets.length) {
      return "";
    }

    if (this.fields.containsKey(id)) {
      return this.fields.get(id).generateJson();
    }

    int fieldOffset = this.fieldOffsets[id];
    this.buffer.position(fieldOffset);
    FieldMeta field = null;
    FieldType type = this.schema.getFieldType(fieldName);
    switch (type) {
      case UserKey:
      case Action:
      case Segment:
        field = new HashFieldMeta(type, this.charset);
        break;
      case Metric:
      case Float:
      case ActionTime:
        field = new RangeFieldMeta(type);
        break;
      default:
        throw new IllegalArgumentException("Unexpected FieldType: " + type);
    }

    this.fields.put(id, field);
    field.readFrom(buffer);
    return field.generateJson();
  }

  @Override
  public String toString() {
    return "cube meta file";
  }
}
