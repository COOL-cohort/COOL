package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Range filter class.
 */
public class RangeFilter implements Filter {

  // Some static defined parameter
  private static final FilterType type = FilterType.Range;
  private static final String MinLimit = "MIN";
  private static final String MaxLimit = "MAX";
  private static final String splitChar = "-";

  // accepted range
  private final List<Scope> acceptRangeList;

  // filter schema
  private final String fieldSchema;

  /**
   * Constructor.
   */
  public RangeFilter(String fieldSchema, String[] acceptRanges) {
    this.fieldSchema = fieldSchema;
    this.acceptRangeList = new ArrayList<Scope>();
    for (String ar : acceptRanges) {
      acceptRangeList.add(RangeFilter.parse(ar));
    }
  }

  /**
   * Need add Scope in the next steps.
   *
   * @param fieldSchema String
   */
  public RangeFilter(String fieldSchema, List<Scope> scopeList) {
    this.fieldSchema = fieldSchema;
    this.acceptRangeList = scopeList;
  }

  /**
   * Check a value.
   *
   * @return True if accepted, false otherwise
   */
  // @Override
  public Boolean accept(RangeField value) {
    for (Scope u : acceptRangeList) {
      if (u.isInScope(value)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check a value.
   *
   * @return True if accepted, false otherwise
   */
  // @Override
  public Boolean accept(RangeField value, Scope selected) {
    for (Scope u : acceptRangeList) {
      if (u.isInScope(value)) {
        selected.copy(u);
        return true;
      }
    }
    return false;
  }
  
  /**
   * check a batch of values.
   *
   * @return a bit map. accepted values have their corresponding bits set.
   */
  // @Override
  public BitSet accept(List<RangeField> values) {
    BitSet res = new BitSet(values.size());
    for (int i = 0; i < values.size(); i++) {
      if (accept(values.get(i))) {
        res.set(i);
      }
    }
    return res;
  }

  @Override
  public Boolean accept(FieldValue value) throws IllegalArgumentException {
    if (!(value instanceof RangeField)) {
      throw new IllegalArgumentException("Invalid value for RangeFilter (RangeField expected)");
    } 
    return accept((RangeField) value);
  }

  /**
   * check if a value range is a subset of the filters'.
   */
  // @Override
  public boolean accept(Scope scope) {
    for (Scope u : acceptRangeList) {
      if (u.isSubset(scope)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public FilterType getType() {
    return type;
  }

  /**
   * Parse string to RangeUnit.
   * Exmaple [145 - 199] = RangeUnit{left:145 , right:199}
   *
   * @param str string
   * @return RangeUnit
   */
  private static Scope parse(String acceptRange) throws IllegalArgumentException {
    String[] part = acceptRange.split(splitChar);
    // Preconditions.checkArgument(part.length == 2,
    //     "Split RangeUnit failed");
    if (part.length != 2) {
      throw new IllegalArgumentException("Range of filter is in invalid form");
    }
    RangeField l = part[0].equals(MinLimit) ? null : ValueWrapper.of(Float.parseFloat(part[0]));
    RangeField r = part[1].equals(MaxLimit) ? null : ValueWrapper.of(Float.parseFloat(part[1]));

    return new Scope(l, r);
  }

  @Override
  public String getFilterSchema() {
    return this.fieldSchema;
  }

  @Override
  public void loadMetaInfo(MetaChunkRS metaChunkRS) {
    // for range Filter, no need to load info
  }
}
