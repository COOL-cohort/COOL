package com.nus.cool.core.cohort.refactor;

import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.filter.FilterType;
import com.nus.cool.core.cohort.refactor.olapSelect.Aggregation;
import com.nus.cool.core.cohort.refactor.olapSelect.olapAggregation;
import com.nus.cool.core.cohort.refactor.olapSelect.olapSelectionLayout;
import com.nus.cool.core.cohort.refactor.olapSelect.olapSelectionLayout.SelectionType;
import com.nus.cool.core.cohort.refactor.olapSelect.olapSelector;
import com.nus.cool.core.cohort.refactor.storage.OlapRet;
import com.nus.cool.core.cohort.refactor.storage.Scope;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.HashMetaFieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import lombok.Data;
import lombok.Getter;

@Data
public class OlapProcessor {

  private OlapQueryLayout query;
  private final String dataSource;
  private olapSelector selection = new olapSelector();

  @Getter
  private final List<OlapRet> result = new ArrayList<>();

  public OlapProcessor(OlapQueryLayout layout){
    this.query = layout;
    this.dataSource = "123";
  }

  /**
   * execute iceberg query
   * timeRange, selection => groupBY => aggregate on each group
   * @param cube the cube that stores the data we need
   * @return the result of the query
   */
  public List<OlapRet> process(CubeRS cube) throws  Exception{

    this.selection.init(this.query);
    for (CubletRS cublet : cube.getCublets()) {
      processCublet(cublet);
    }
    return this.result;
  }

  /**
   * Process each cublet
   * @param cubletRS cubletRS
   */
  private void processCublet(CubletRS cubletRS){
    MetaChunkRS metaChunk = cubletRS.getMetaChunk();

    // if this cublet don't meet fild requirements
    if (!this.checkMetaChunk(metaChunk, selection.getSelection())) {
      return;
    }

    for (ChunkRS dataChunk : cubletRS.getDataChunks()) {
      if (this.checkDataChunk(dataChunk, selection.getSelection() )){
        this.processDataChunk(metaChunk, dataChunk);
      }
    }
  }

  /**
   * Check if this cublet contains the required field.
   * @param metaChunk hashMetaFields result
   * @param selectionFilter selection filter
   * @return true: this metaChunk is valid, false: this metaChunk is invalid.
   */
  private Boolean checkMetaChunk(MetaChunkRS metaChunk, olapSelectionLayout selectionFilter){

    if (selectionFilter == null) return true;

    if (selectionFilter.getType().equals(SelectionType.filter)){

      // get current filter and metaField
      Filter currentFilter = selectionFilter.getFilter();
      MetaFieldRS metaField = metaChunk.getMetaField(selectionFilter.getDimension());

      return this.checkMetaField(metaField, currentFilter);

    }
    // recursively check if this cublet requires checking
    else{
      boolean flag = selectionFilter.getType().equals(SelectionType.and);

      for(olapSelectionLayout childSelection: selectionFilter.getFields()){
        if (selectionFilter.getType().equals(SelectionType.and)){
          flag &= this.checkMetaChunk(metaChunk, childSelection);
        }else{
          flag |= this.checkMetaChunk(metaChunk, childSelection);
        }
      }
      return flag;
    }
  }

  /**
   * Check if dataChunk meet the requirements
   * @param dataChunk dataChunk
   * @param selectionFilter selecton Filter
   * @return t /f
   */
  private boolean checkDataChunk(ChunkRS dataChunk, olapSelectionLayout selectionFilter) {
    if (selectionFilter == null) return true;
    if (selectionFilter.getType().equals(SelectionType.filter)) {

      FieldRS field = dataChunk.getField(selectionFilter.getDimension());
      Filter currentFilter = selectionFilter.getFilter();

      if (currentFilter.getType().equals(FilterType.Range)){
        Scope scope = new Scope(field.minKey(), field.maxKey());
        BitSet res = currentFilter.accept(scope);
        return res.nextSetBit(0) != -1;
      }
      // todo: how to check using Set Filter in dataChunk
      return true;

    } else {
      boolean flag = selectionFilter.getType().equals(SelectionType.and);
      for (olapSelectionLayout childFilter : selectionFilter.getFields()) {
        if (selectionFilter.getType().equals(SelectionType.and)) {
          flag &= checkDataChunk(dataChunk, childFilter);
        } else {
          flag |= checkDataChunk(dataChunk, childFilter);
        }
      }
      return flag;
    }
  }

  /**
   * check if this metaFiled meet the requirement
   * @param metaField metaField
   * @param ft filter
   * @return yes, no
   */
  public Boolean checkMetaField(MetaFieldRS metaField, Filter ft) {
    FilterType checkedType = ft.getType();
    if (checkedType.equals(FilterType.Set)) {
      BitSet res = ft.accept(( (HashMetaFieldRS) metaField).getGidMap());
      // if there is no true in res, then no record meet the requirement
      return res.nextSetBit(0) != -1;
    } else if (checkedType.equals(FilterType.Range)) {
      Scope scope = new Scope(metaField.getMinValue(), metaField.getMaxValue());
      BitSet res = ft.accept(scope);
      // if there is no true in res, then no record meet the requirement
      return res.nextSetBit(0) != -1;
    } else {
      throw new IllegalArgumentException("Only support set or range");
    }
  }

  /**
   * Process data chunk
   * @param metaChunk meta chunk
   * @param dataChunk data chunk
   */
  private void processDataChunk(MetaChunkRS metaChunk, ChunkRS dataChunk){
    // 1. find all records in dataChunk meet the timeRange and selection requirements.
    BitSet bs = this.selection.selectRecordsOnDataChunk(dataChunk);
    if (bs.nextSetBit(0) == -1){
      return ;
    }

    // 2. run groupBy
    olapAggregation olapAggregator = new olapAggregation();
    olapAggregator.groupBy(
        bs, this.query.getGroupFields(), metaChunk, dataChunk, query.getGroupFields_granularity());

    for (Aggregation aggregation : this.query.getAggregations()) {
      List<OlapRet> res = olapAggregator.process(aggregation);
      this.result.addAll(res);
    }
  }


}
