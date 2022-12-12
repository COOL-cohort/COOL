package com.nus.cool.core.cohort.filter;

import com.google.common.base.Preconditions;
import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.util.converter.DayIntConverter;
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
  private static final String TimeDelimiter = "-";
  @Getter
  private static final String splitChar = " to ";

  // accepted range
  @Getter
  private final List<Scope> acceptRangeList;

  // filter schema
  private final String fieldSchema;

  /**
   * Constructor.
   *
   * @param fieldSchema  fieldSchema
   * @param acceptValues acceptValues
   */
  public RangeFilter(String fieldSchema, String[] acceptValues) {
    this.fieldSchema = fieldSchema;
    this.acceptRangeList = new ArrayList<Scope>();
    for (String acceptValue : acceptValues) {
      acceptRangeList.add(RangeFilter.parse(acceptValue));
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

  @Override
  public Boolean accept(Integer value) throws RuntimeException {
    for (Scope u : acceptRangeList) {
      if (u.isInScope(value)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public BitSet accept(List<Integer> values) throws RuntimeException {
    BitSet res = new BitSet(values.size());
    for (int i = 0; i < values.size(); i++) {
      if (accept(values.get(i))) {
        res.set(i);
      }
    }
    return res;
  }

  // --------------- compatable with old version -----------------

  public Boolean accept(String value) throws RuntimeException {
    // TODO Auto-generated method stub
    return null;
  }

  public BitSet accept(String[] values) throws RuntimeException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean accept(Scope scope) throws RuntimeException {
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
   * @param str string
   * @return RangeUnit
   */
  private static Scope parse(String str) {
    String[] part = str.split(splitChar);
    Preconditions.checkArgument(part.length == 2, "Split RangeUnit failed");
    Integer l = null;
    Integer r = null;
    if (!part[0].equals(MinLimit)) {
      l = convertPartToInt(part[0]);
    }
    if (!part[1].equals(MaxLimit)) {
      r = convertPartToInt(part[1]);
    }
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

  private static int convertPartToInt(String dataStr) {
    if (dataStr.contains(RangeFilter.TimeDelimiter)) {
      DayIntConverter dins = DayIntConverter.getInstance();
      return dins.toInt(dataStr);
    } else {
      return Integer.parseInt(dataStr);
    }
  }


}