package com.nus.cool.core.util.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.nus.cool.core.cohort.KeyFieldIterator;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.HashMetaFieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.converter.DayIntConverter;

// if the field is marked with PreCAL, we will not be able to reconstruct the tuple

public class CoolTupleReader implements TupleReader {
  
  private final TableSchema tableSchema;

  // contains value mapping for all data chunks
  private final MetaChunkRS metaChunk; // the metachunk

  // all data chunks should be governed by the same metachunk
  private final List<ChunkRS> datachunks;

  // only tuples of users emitted
  private final InputVector users;

  /**
   * initialized once
   */

  private final int userKeyFieldIdx;

  private final List<ValueConverter> valueConverters;
  /**
   * variables describing current state
   */
  private boolean hasNext;

  private final ListIterator<ChunkRS> chunkItr;

  private ChunkRS curChunk;

  private final List<InputVector> fields;

  private KeyFieldIterator curChunkUserItr;

  private int curUser = -1;

  private int curTupleOffset = -1;

  private int ValidTupleOffsetUntil = -1;

  public CoolTupleReader(CubeRS cube) {
    this(cube, null);
  }
  
  public CoolTupleReader(CubeRS cube, InputVector users) {
    this.tableSchema = cube.getSchema();
    this.datachunks = new ArrayList<>();
    List<CubletRS> cublets = cube.getCublets();
    for (CubletRS cublet : cublets) {
      datachunks.addAll(cublet.getDataChunks());
    }
    // assuming the last cublet having an encompassing metachunk
    this.metaChunk = cublets.get(cublets.size()-1).getMetaChunk();
    this.valueConverters = createValueConverters();
    this.users = users;
    if (this.users != null && this.users.hasNext()) {
      curUser = this.users.next();
    } 
    this.userKeyFieldIdx = this.tableSchema.getUserKeyField();
    this.chunkItr = datachunks.listIterator();
    this.curChunk = null;
    this.fields = new ArrayList<>();
    this.hasNext = (chunkItr.hasNext()) ? skipToNextUser() : false;
  }

  interface ValueConverter {
    String convert (int value);

    public static ValueConverter createNullConverter() {
      return new ValueConverter() {
        @Override
        public String convert(int value) {
          return "NULL";
        }
      };
    }
  }

  private List<ValueConverter> createValueConverters() {
    List<ValueConverter> converters = new ArrayList<>();
    for (FieldSchema fieldSchema : tableSchema.getFields()) {
      if(fieldSchema.isPreCal()) {
        converters.add(ValueConverter.createNullConverter());
      } else {
        switch (fieldSchema.getFieldType()) {
          case AppKey:
          case UserKey:
          case Action:
          case Segment:
            converters.add(new ValueConverter() {
              private final HashMetaFieldRS valueVec = 
                (HashMetaFieldRS) metaChunk.getMetaField(
                  fieldSchema.getName());
              @Override
              public String convert(int value) {
                return valueVec.getString(value);
              }
            });
            break;
          case ActionTime:
            converters.add(new ValueConverter() {
              private final DayIntConverter converter = new DayIntConverter();
              @Override
              public String convert(int value) {
                return converter.getString(value);
              }
            });
            break;
          case Metric:
            converters.add(new ValueConverter() {
              @Override
              public String convert(int value) {
                return String.valueOf(value);
              }
            });
            break;
          default:
            System.out.println("Unknown field type");
            break;
        }
      }
    }
    return converters;
  }

  // return false when there is no more chunk
  private boolean switchToNextChunk() {
    if (!chunkItr.hasNext()) return false;
    curChunk = chunkItr.next();
    curChunkUserItr = new KeyFieldIterator.Builder(
        curChunk.getField(userKeyFieldIdx)).build().get();
    fields.clear();
    for (FieldSchema fieldSchema : tableSchema.getFields()) {
      fields.add(curChunk.getField(fieldSchema.getName()).getValueVector());
    }
    // if we cannot iterate over the user in current chunk 
    //  (corrupted user field) we skip to the next chunk 
    return (curChunkUserItr == null) ? switchToNextChunk() : true;
  }

  private boolean skipToNextUser() {
    // for first time invocation
    if ((curChunk == null) && (!switchToNextChunk())) return false;
    
    // if ((curChunk == null || curChunkUserItr.next())
    //   && (!switchToNextChunk())) {
    //   return false;
    // } 

    // we have valid chunk user itr
    //  looping users, when a chunk user itr reached the end,
    //  switch to a new valid chunk
    while (curChunkUserItr.next() 
      || (switchToNextChunk() && curChunkUserItr.next())) {
      
      if (users != null) {
        if (curUser < 0) return false; // we have no more users to emit records for
        if (curUser != curChunkUserItr.key()) {
          continue;
        }
        // move to next target user the next time.
        curUser = (users.hasNext()) ? users.next() : -1; 
      }
      // set the current tuple offset and the validity boundary
      curTupleOffset = curChunkUserItr.getStartOffset();
      ValidTupleOffsetUntil = curChunkUserItr.getEndOffset() - 1; 
      return true;
    }
    return false;
  }

  // move to the next record
  private boolean skipToNext() {
    // the offsets will not be smaller than zero after initialization
    if (curTupleOffset >= ValidTupleOffsetUntil) {
      return skipToNextUser();
    }
    curTupleOffset++;
    return true;
  }

  private String[] getCurrentTuple() {
    int numField = fields.size();
    String[] ret = new String[numField];
    for (int i = 0; i < numField; i++) {
      ret[i] = (fields.get(i) == null)
        ? "PreCAL" // for PreCAL field, no value vector will be initialized
        : valueConverters.get(i).convert(fields.get(i).get(curTupleOffset)); 
    }
    return ret;
  }
  
  @Override
  public boolean hasNext() {
    return this.hasNext;
  }
  
  @Override
  public Object next() throws IOException {
    String[] old = getCurrentTuple();
    this.hasNext = skipToNext();
    return old;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
