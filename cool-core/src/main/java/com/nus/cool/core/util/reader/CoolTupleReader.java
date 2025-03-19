package com.nus.cool.core.util.reader;

import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.StringHashField;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaHashFieldRS;
import com.nus.cool.core.io.readstore.MetaUserFieldRS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.converter.ActionTimeIntConverter;
import com.nus.cool.core.util.converter.SecondIntConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

// if the field is marked with PreCAL, we will not be able to reconstruct the tuple

/**
 * Tuple reader of cool data format.
 */
public class CoolTupleReader implements TupleReader {

  private final TableSchema tableSchema;

  // contains value mapping for all data chunks
  private final MetaChunkRS metaChunk; // the metachunk

  // all data chunks should be governed by the same metachunk
  private final List<ChunkRS> datachunks;

  // only tuples of users emitted
  // we no longer maintain user key sorted assumption
  // if we are to maintain chunk or cube sorted that is another matter,
  // we can revert back to a sorted list of users as filter input.
  private final Set<String> users;

  // initialized once
  private final int userKeyFieldIdx;

  private final ActionTimeIntConverter actionTimeConverter;

  private final ValueConverter userValueConverter;

  private final List<ValueConverter> valueConverters;

  // variables describing current state
  private boolean hasNext;

  private final ListIterator<ChunkRS> chunkItr;

  private ChunkRS curChunk;

  private final List<FieldRS> fields;

  // private KeyFieldIterator curChunkUserItr;
  private FieldRS curUserField;

  private int lastUserId = -1;

  private int curTupleOffset = -1;

  private int validTupleOffsetUntil = -1;

  public CoolTupleReader(CubeRS cube) {
    this(cube, new HashSet<>(), new SecondIntConverter());
  }

  public CoolTupleReader(CubeRS cube, Set<String> users) {
    this(cube, users, new SecondIntConverter());
  }

  /**
   * Create a tuple reader for a cube and with a list of users as filter.
   */
  public CoolTupleReader(CubeRS cube, Set<String> users,
      ActionTimeIntConverter actionTimeConverter) {
    this.tableSchema = cube.getSchema();
    this.datachunks = new ArrayList<>();
    List<CubletRS> cublets = cube.getCublets();
    for (CubletRS cublet : cublets) {
      datachunks.addAll(cublet.getDataChunks());
    }
    // assuming the last cublet having an encompassing metachunk
    this.metaChunk = cublets.get(cublets.size() - 1).getMetaChunk();
    this.userValueConverter = new ValueConverter() {
      private final MetaUserFieldRS valueVec = (MetaUserFieldRS) metaChunk
          .getMetaField(tableSchema.getUserKeyFieldName());

      @Override
      public FieldValue convert(FieldValue value) {
        // return valueVec.get(value.getInt()).map(FieldValue::getString).orElse(null);
        return valueVec.get(value.getInt()).orElse(null);
      }
    };
    this.valueConverters = createValueConverters();
    this.users = users;
    // if (this.users != null && this.users.hasNext()) {
    //   curUser = this.users.next();
    // }
    this.userKeyFieldIdx = this.tableSchema.getUserKeyFieldIdx();
    this.chunkItr = datachunks.listIterator();
    this.curChunk = null;
    this.fields = new ArrayList<>();
    this.hasNext = (chunkItr.hasNext()) ? switchToNextChunk() : false;
    this.actionTimeConverter = actionTimeConverter;
  }

  interface ValueConverter {
    FieldValue convert(FieldValue value);

    // public static ValueConverter createNullConverter() {
    //   // return new ValueConverter() {
    //   //   @Override
    //   //   public String convert(int value) {
    //   //     return "NULL";
    //   //   }
    //   // };
    //   return x -> "NULL";
    // }
  }

  private List<ValueConverter> createValueConverters() {
    List<ValueConverter> converters = new ArrayList<>();
    for (FieldSchema fieldSchema : tableSchema.getFields()) {
      // if (fieldSchema.isPreCal()) {
      // converters.add(ValueConverter.createNullConverter());
      // } else {
      switch (fieldSchema.getFieldType()) {
        case UserKey:
          converters.add(this.userValueConverter);
          break;
        case AppKey:
        case Action:
        case Segment:
          converters.add(new ValueConverter() {
            private final MetaHashFieldRS valueVec = (MetaHashFieldRS) metaChunk
                .getMetaField(fieldSchema.getName());

            @Override
            public FieldValue convert(FieldValue value) {
              return valueVec.get(value.getInt()).orElse(null);
            }
          });
          break;
        case ActionTime:
          converters.add(x -> new StringHashField(actionTimeConverter.getString(x.getInt())));
          break;
        case Metric:
        case Float:
          converters.add(x -> x);
          break;
        default:
          System.out.println("Unknown field type");
          break;
      }
      // }
    }
    return converters;
  }

  // return false when there is no more chunk
  private boolean switchToNextChunk() {
    if (!chunkItr.hasNext()) {
      return false;
    }
    curChunk = chunkItr.next();
    curUserField = curChunk.getField(userKeyFieldIdx);
    fields.clear();
    for (FieldSchema fieldSchema : tableSchema.getFields()) {
      fields.add(curChunk.getField(fieldSchema.getName()));
    }
    // if we cannot iterate over the user in current chunk
    // (corrupted user field) we skip to the next chunk
    curTupleOffset = 0;
    validTupleOffsetUntil = curChunk.getRecords();
    while (curTupleOffset < validTupleOffsetUntil) {
      FieldValue curUser = curUserField.getValueByIndex(curTupleOffset);
      if (users.isEmpty() || users.contains(this.userValueConverter.convert(curUser).getString())) {
        lastUserId = curUser.getInt();
        return true;
      }
      curTupleOffset++;
    }
    return switchToNextChunk();
  }

  // move to the next record
  private boolean skipToNext() {
    while (++curTupleOffset < validTupleOffsetUntil) {
      FieldValue curUser = curUserField.getValueByIndex(curTupleOffset);
      int curUserId = curUser.getInt();
      if (curUserId == lastUserId) {
        // fast path to skip check in users.
        return true;
      } else if (users.isEmpty()
          || users.contains(this.userValueConverter.convert(curUser).getString())) {
        lastUserId = curUserId;
        return true;
      }
    }
    return switchToNextChunk();
  }

  private FieldValue[] getCurrentTuple() {
    int numField = fields.size();
    FieldValue[] ret = new FieldValue[numField];
    for (int i = 0; i < numField; i++) {
      ret[i] = (fields.get(i) == null) ? null
          : valueConverters.get(i).convert(fields.get(i).getValueByIndex(curTupleOffset));
    }
    return ret;
  }

  @Override
  public boolean hasNext() {
    return this.hasNext;
  }

  @Override
  public Object next() throws IOException {
    // String[] old = getCurrentTuple();
    FieldValue[] old = getCurrentTuple();
    this.hasNext = skipToNext();
    return old;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
