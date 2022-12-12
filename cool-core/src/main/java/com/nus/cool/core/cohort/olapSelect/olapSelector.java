package com.nus.cool.core.cohort.olapSelect;

import com.nus.cool.core.cohort.OlapQueryLayout;
import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.olapSelect.olapSelectionLayout.SelectionType;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class olapSelector {

  @Data
  @AllArgsConstructor
  public static class TimeBitSet{
    private String timeRange;
    private BitSet matchedRecords;
  }

  private olapSelectionLayout selection;

  private String timeRange;

  // max and min query Time specified by user
  private int maxQueryTime;
  private int minQueryTime;

  // time range split by system, used as key in map
  private List<String> timeRanges;

  // store multiple time range lower bounds and upper bounds, convert data into integer
  private List<Integer> maxTimeRanges;
  private List<Integer> minTimeRanges;


  public void init(OlapQueryLayout query) throws ParseException {
    this.selection = query.getSelection();
  }

  /**
   * process dataChunk filter with time range
   * @param dataChunk: dataChunk
   * @return map of < time_range : bitMap >,
   *  where each element in biteMap is one record,
   *              true = the record meet the requirement,
   *              false = the record don't meet the requirement.
   */
  public BitSet selectRecordsOnDataChunk(ChunkRS dataChunk) {

    // if the query don't provide timeRange, all record is true
    BitSet bv = new BitSet(dataChunk.records());
    bv.set(0, dataChunk.records());

    return select(this.selection, dataChunk, bv );
  }

  /**
   * Recursively use the filter
   * @param selectionFilter selectionFilter
   * @param chunk data-chunk
   * @param bv
   * @return BitSet
   */
  private BitSet select(olapSelectionLayout selectionFilter, ChunkRS chunk, BitSet bv) {
    BitSet bs = (BitSet) bv.clone();
    if (selectionFilter == null) return bs;
    // if this is the final filter.
    if (selectionFilter.getType().equals(SelectionType.filter)) {
      FieldRS field = chunk.getField(selectionFilter.getDimension());
      selectFields(bs, field, selectionFilter.getFilter());
    }
    // for and operator
    else if (selectionFilter.getType().equals(SelectionType.and)) {
      for (olapSelectionLayout childFilter : selectionFilter.getFields()) {
        bs = select(childFilter, chunk, bs);
      }
    }
    // for or operator
    else if (selectionFilter.getType().equals(SelectionType.or)) {
      List<BitSet> bitSets = new ArrayList<>();
      for (olapSelectionLayout childFilter : selectionFilter.getFields()) {
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

  private void selectFields(BitSet bs, FieldRS field, Filter filter) {
    // get local ids
    InputVector fieldIn = field.getValueVector();
    int off = 0;
    while (off < fieldIn.size() && off >= 0) {
      fieldIn.skipTo(off);
      if(!filter.accept(fieldIn.next())) {
        bs.clear(off);
      }
      off = bs.nextSetBit(off + 1);
    }
  }
}
