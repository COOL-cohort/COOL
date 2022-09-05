package com.nus.cool.core.cohort.refactor.olapSelect;

import com.nus.cool.core.cohort.refactor.OlapQueryLayout;
import com.nus.cool.core.cohort.refactor.OlapQueryLayout.granularityType;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.olapSelect.olapSelectionLayout.SelectionType;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.TableSchema;
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

  private TableSchema tableSchema;

  private olapSelectionLayout selection;

  private Filter filter;


  private String timeRange;

  // max and min query Time specified by user
  private int maxQueryTime;
  private int minQueryTime;

  // time range split by system, used as key in map
  private List<String> timeRanges;

  // store multiple time range lower bounds and upper bounds, convert data into integer
  private List<Integer> maxTimeRanges;
  private List<Integer> minTimeRanges;


  public void init(TableSchema tableSchema, OlapQueryLayout query) throws ParseException {
    this.tableSchema = tableSchema;
    this.selection = query.getSelection();
    this.selection.initSelectionFilter();
    granularityType groupFields_granularity = query.getGroupFields_granularity();

  }

  /**
   * process dataChunk filter with time range
   * @param dataChunk: dataChunk
   * @return map of < time_range : bitMap >,
   *  where each element in biteMap is one record,
   *              true = the record meet the requirement,
   *              false = the record don't meet the requirement.
   */
  public ArrayList<TimeBitSet> processDataChunk(ChunkRS dataChunk) {

    // store the records in map of ["t1|t2": bitSets [t,f...] ]
    ArrayList< TimeBitSet > resultMap = new ArrayList<>();

    // if the query don't provide timeRange, all record is true
    BitSet bv = new BitSet(dataChunk.records());
    bv.set(0, dataChunk.records());
    resultMap.add( new TimeBitSet("no time filter", bv ) );

    for (int i = 0; i < resultMap.size(); i++){
      BitSet bs = select(this.selection, dataChunk, resultMap.get(i).getMatchedRecords());
      resultMap.set(i, new TimeBitSet( this.timeRanges.get(i), bs ) );
    }

    return resultMap;
  }

  /**
   * Recursively use the filter
   * @param selectionFilter selectionFilter
   * @param chunk data-chunk
   * @param bv
   * @return
   */
  private BitSet select(olapSelectionLayout selectionFilter, ChunkRS chunk, BitSet bv) {
    BitSet bs = (BitSet) bv.clone();
    if (selectionFilter == null) return bs;
    if (selectionFilter.getType().equals(SelectionType.filter)) {
      FieldRS field = chunk.getField(this.tableSchema.getFieldID(selectionFilter.getDimension()));
      InputVector keyVector = field.getKeyVector();
      selectFields(bs, field, selectionFilter.getFilter());

    } else if (selectionFilter.getType().equals(SelectionType.and)) {
      for (olapSelectionLayout childFilter : selectionFilter.getFields()) {
        bs = select(childFilter, chunk, bs);
      }
    } else if (selectionFilter.getType().equals(SelectionType.or)) {
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
    InputVector fieldIn = field.getValueVector();
    //fieldIn.skipTo(beg);
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
