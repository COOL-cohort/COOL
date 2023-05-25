package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.field.RangeField;
import com.nus.cool.core.field.ValueWrapper;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.util.converter.SecondIntConverter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import lombok.Getter;

/**
 * Range filter class.
 */
public class RangeFilter implements Filter {

  // Some static defined parameter
  private static final FilterType type = FilterType.Range;
  private static final String MinLimit = "MIN";
  private static final String MaxLimit = "MAX";
  @Getter
  private static final String splitChar = " to ";

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
  public boolean accept(Scope scope) {
    for (Scope u : acceptRangeList) {
      if (u.isSubset(scope) || u.isIntersection(scope)) {
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
   * @param acceptRange string
   * @return RangeUnit
   */
  private static Scope parse(String acceptRange) throws IllegalArgumentException {
    String[] part = acceptRange.split(splitChar);
    if (part.length != 2) {
      throw new IllegalArgumentException("Range of filter is in invalid form");
    }


    RangeField l;
    RangeField r;
    try {
      l = part[0].equals(MinLimit) ? null : ValueWrapper.of(Float.parseFloat(part[0]));
      r = part[1].equals(MaxLimit) ? null : ValueWrapper.of(Float.parseFloat(part[1]));
    } catch (Exception e)  {
      System.out.println("[Warning]. Parse using float failed, element = " + part[0]);
      SecondIntConverter secondConverter = new SecondIntConverter();
      int intValueMin = secondConverter.toInt(part[0]);
      int intValueMax = secondConverter.toInt(part[1]);
      l = part[0].equals(MinLimit) ? null : ValueWrapper.of(intValueMin);
      r = part[1].equals(MaxLimit) ? null : ValueWrapper.of(intValueMax);
    }

    return new Scope(l, r);
  }

  @Override
  public String getFilterSchema() {
    return this.fieldSchema;
  }

  @Override
  public void loadMetaInfo(MetaChunkRS metaChunkRs) {
    // for range Filter, no need to load info
  }


}
