package com.nus.cool.core.cohort.refactor.filter;

import com.nus.cool.core.cohort.refactor.storage.Scope;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;

/**
 * Set filter.
 */
public class SetFilter implements Filter {

  protected final FilterType type = FilterType.Set;

  protected HashSet<String> valueSet;
  protected final String fieldSchema;
  protected HashSet<Integer> gidSet;

  protected SetFilter(String fieldSchema, String[] values) {
    this.fieldSchema = fieldSchema;
    this.valueSet = new HashSet<>(Arrays.asList(values));
  }

  /**
   * Create instance of SetFilter.
   */
  public static Filter generateSetFilter(String fieldSchema,
      String[] acceptValues, String[] rejectValues) {
    if (acceptValues != null) {
      return new SetAcceptFilter(fieldSchema, acceptValues);
    } else if (rejectValues != null) {
      return new SetRejectFilter(fieldSchema, rejectValues);
    } else {
      throw new IllegalArgumentException(
          "For SetFilter, acceptValue and rejectValue aren't equal to null at the same time");
    }
  }

  @Override
  public Boolean accept(Integer value) throws RuntimeException {
    throw new UnsupportedOperationException(
        "ChildClass of SetFilter should override to implement Accept");
  }

  @Override
  public Boolean accept(String value) throws RuntimeException {
    throw new UnsupportedOperationException(
        "ChildClass of SetFilter should override to implement Accept");
  }

  @Override
  public BitSet accept(String[] values) throws RuntimeException {
    BitSet res = new BitSet(values.length);
    for (int i = 0; i < values.length; i++) {
      if (this.accept(values[i])) {
        res.set(i);
      }
    }
    return res;
  }

  @Override
  public BitSet accept(List<Integer> values) throws RuntimeException {
    BitSet res = new BitSet(values.size());
    for (int i = 0; i < values.size(); i++) {
      if (this.accept(values.get(i))) {
        res.set(i);
      }
    }
    return res;
  }

  @Override
  public boolean accept(Scope scope) throws RuntimeException {
    for (int i = scope.getLeft(); i < scope.getRight(); i++) {
      if (!this.accept(i)) {
        return false;
      }
    }
    return true;
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
