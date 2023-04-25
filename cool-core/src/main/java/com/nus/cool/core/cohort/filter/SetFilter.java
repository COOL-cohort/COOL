package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;

/**
 * Set filter.
 */
public abstract class SetFilter implements Filter {

  protected final FilterType type = FilterType.Set;

  protected HashSet<String> valueSet;
  protected final String fieldSchema;
  protected HashSet<Integer> gidSet;

  protected SetFilter(String fieldSchema, String[] values) {
    this.fieldSchema = fieldSchema;
    this.valueSet = new HashSet<>(Arrays.asList(values));
  }

  /**
   * Create an empty SetFilter.
   */
  public static SetFilter generateEmptySetFilter(String fieldSchema) {
    return new SetFilter(fieldSchema, new String[0]) {
      @Override
      public Boolean accept(Integer value) {
        return true;
      }

      @Override
      public Boolean accept(String value) {
        return true;
      }

      @Override
      public BitSet accept(String[] values) {
        BitSet res = new BitSet(values.length);
        res.set(0, values.length);
        return res;
      }
    };
  }

  // filter based on global id
  protected abstract Boolean accept(Integer value) throws IllegalStateException;

  // filter based on value
  protected abstract Boolean accept(String value);

  @Override
  public Boolean accept(FieldValue v) throws IllegalArgumentException, IllegalStateException {
    switch (v.getType()) {
      case Metric:
        return accept(v.getInt());
      case Segment:
        return accept(v.getString());
      default:
        throw new IllegalArgumentException(
            "Invalid argument for SetFilter (IntRangeField or HashField expected)");
    }
  }

  /**
   * check a batch of values.
   *
   * @return a bit map. accepted values have their corresponding bits set.
   */
  // @Override
  public BitSet accept(String[] values) {
    BitSet res = new BitSet(values.length);
    for (int i = 0; i < values.length; i++) {
      if (this.accept(values[i])) {
        res.set(i);
      }
    }
    return res;
  }

  @Override
  public FilterType getType() {
    return this.type;
  }

  @Override
  public String getFilterSchema() {
    return this.fieldSchema;
  }

  @Override
  public void loadMetaInfo(MetaChunkRS metaChunkRS) {
    this.gidSet = new HashSet<>();
    MetaFieldRS metaFieldRS = metaChunkRS.getMetaField(this.fieldSchema);
    for (String value : this.valueSet) {
      int gid = metaFieldRS.find(value);
      if (gid == -1) {
        // means this value is not existed in this Cublet
        continue;
      }
      this.gidSet.add(gid);
    }
  }
}
