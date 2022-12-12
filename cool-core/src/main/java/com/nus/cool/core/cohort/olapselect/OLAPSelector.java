package com.nus.cool.core.cohort.olapselect;

import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.olapselect.OLAPSelectionLayout.SelectionType;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * OLAPSelector.
 */
@Data
public class OLAPSelector {

  /**
   * TimeBitSet.
   */
  @Data
  @AllArgsConstructor
  public static class TimeBitSet {
    private String timeRange;
    private BitSet matchedRecords;
  }

  /**
   * process dataChunk filter with time range.
   *
   * @param dataChunk dataChunk
   * @return map of < time_range : bitMap >,
   *      where each element in biteMap is one record,
   *      true = the record meet the requirement,
   *      false = the record don't meet the requirement.
   */
  public BitSet selectRecordsOnDataChunk(OLAPSelectionLayout selectionFilter, ChunkRS dataChunk) {

    // init the result. all records are matched by default.
    BitSet bv = new BitSet(dataChunk.records());
    bv.set(0, dataChunk.records());
    // recursively use the filter.
    return select(selectionFilter, dataChunk, bv);
  }

  /**
   * Recursively use the filter.
   *
   * @param selectionFilter selectionFilter
   * @param chunk           data-chunk
   * @param bv bv
   * @return BitSet
   */
  private BitSet select(OLAPSelectionLayout selectionFilter, ChunkRS chunk, BitSet bv) {
    BitSet bs = (BitSet) bv.clone();
    if (selectionFilter == null) {
      return bs;
    }
    if (selectionFilter.getType().equals(SelectionType.filter)) {
      // if this is the final filter, run select on it.
      FieldRS field = chunk.getField(selectionFilter.getDimension());
      int chunkSize = chunk.getRecords();
      selectFromOneColumn(bs, field, chunkSize, selectionFilter.getFilter());
    } else if (selectionFilter.getType().equals(SelectionType.and)) {
      // for and operator
      for (OLAPSelectionLayout childFilter : selectionFilter.getFields()) {
        bs = select(childFilter, chunk, bs);
      }
    } else if (selectionFilter.getType().equals(SelectionType.or)) {
      // for or operator
      List<BitSet> bitSets = new ArrayList<>();
      for (OLAPSelectionLayout childFilter : selectionFilter.getFields()) {
        bitSets.add(select(childFilter, chunk, bs));
      }
      bs = this.orBitSets(bitSets);
    }
    return bs;
  }

  private BitSet orBitSets(List<BitSet> bitSets) {
    BitSet bs = bitSets.get(0);
    for (int i = 1; i < bitSets.size(); i++) {
      bs.or(bitSets.get(i));
    }
    return bs;
  }

  // run selection on each file
  private void selectFromOneColumn(BitSet bs, FieldRS field, int chunkSize, Filter filter) {

    // for each record
    for (int i = 0; i < chunkSize; i++) {
      if (!filter.accept(field.getValueByIndex(i))) {
        bs.clear(i);
      }
    }
  }
}
