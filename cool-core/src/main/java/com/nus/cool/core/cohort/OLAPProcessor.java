package com.nus.cool.core.cohort;

import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.filter.FilterType;
import com.nus.cool.core.cohort.filter.RangeFilter;
import com.nus.cool.core.cohort.olapselect.AggregationLayout;
import com.nus.cool.core.cohort.olapselect.OLAPAggregator;
import com.nus.cool.core.cohort.olapselect.OLAPGroupBy;
import com.nus.cool.core.cohort.olapselect.OLAPSelectionLayout;
import com.nus.cool.core.cohort.olapselect.OLAPSelectionLayout.SelectionType;
import com.nus.cool.core.cohort.olapselect.OLAPSelector;
import com.nus.cool.core.cohort.storage.OLAPRet;
import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.DataRangeFieldRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import lombok.Data;
import lombok.Getter;

/**
 * OLAP Query Processing Engine.
 */
@Data
public class OLAPProcessor {

  // points to layouts and
  private OLAPQueryLayout query;

  private List<OLAPRet> result = new ArrayList<>();

  @Getter
  private final HashSet<String> projectedSchemaSet = new HashSet<>();

  // Constructor
  public OLAPProcessor(OLAPQueryLayout layout) {
    this.query = layout;
    this.projectedSchemaSet.addAll(layout.getSchemaSet());
  }

  /**
   * Execute iceberg query over each cublet.
   *
   * @param cube the cube that stores the data we need
   * @return the result of the query
   */
  public List<OLAPRet> processCube(CubeRS cube) {
    TableSchema tableschema = cube.getSchema();
    // add this two schema into list to be compatible with existing aggregator
    for (FieldSchema fieldSchema : tableschema.getFields()) {
      if (fieldSchema.getFieldType() == FieldType.UserKey) {
        this.projectedSchemaSet.add(fieldSchema.getName());
      } else if (fieldSchema.getFieldType() == FieldType.ActionTime) {
        this.projectedSchemaSet.add(fieldSchema.getName());
      }
    }
    // process each cublet
    for (CubletRS cublet : cube.getCublets()) {
      // 1. update filter condition according to current metaChunk
      MetaChunkRS metaChunk = cublet.getMetaChunk();
      this.query.getSelection().updateFilterCacheInfo(metaChunk);
      // 2. check if this cublet requires filter requirement.
      if (this.checkMetaChunk(metaChunk, this.query.getSelection())) {
        // 3. process this cublet dataChunk
        for (ChunkRS dataChunk : cublet.getDataChunks()) {
          // 4. check dataChunk
          if (this.checkDataChunk(dataChunk, this.query.getSelection())) {
            // 5. process each data Chunk
            this.processDataChunk(metaChunk, dataChunk);
          }
        }
      }
    }
    return this.result;
  }

  /**
   * Check if this cublet contains the required field.
   *
   * @param metaChunk       hashMetaFields result
   * @param selectionFilter selection filter, this is for recursively traverse the selection.
   * @return true: this metaChunk is valid, false: this metaChunk is invalid.
   */
  private Boolean checkMetaChunk(MetaChunkRS metaChunk, OLAPSelectionLayout selectionFilter) {

    if (selectionFilter == null) {
      return true;
    }
    if (selectionFilter.getType().equals(SelectionType.filter)) {
      // get current filter and metaField
      MetaFieldRS metaField = metaChunk.getMetaField(selectionFilter.getDimension());
      Filter currentFilter = selectionFilter.getFilter();
      return this.checkMetaField(metaField, currentFilter);
    } else {
      // recursively check if this cublet requires checking
      boolean flag = selectionFilter.getType().equals(SelectionType.and);
      for (OLAPSelectionLayout childSelection : selectionFilter.getFields()) {
        if (selectionFilter.getType().equals(SelectionType.and)) {
          flag &= this.checkMetaChunk(metaChunk, childSelection);
        } else {
          flag |= this.checkMetaChunk(metaChunk, childSelection);
        }
      }
      return flag;
    }
  }

  /**
   * check if this metaFiled meet the requirement.
   *
   * @param metaField metaField
   * @param ft        filter
   * @return yes, no
   */
  private Boolean checkMetaField(MetaFieldRS metaField, Filter ft) {
    FilterType checkedType = ft.getType();
    if (checkedType.equals(FilterType.Set)) {
      // if this is set, just check it.
      return true;
    } else if (checkedType.equals(FilterType.Range)) {
      Scope scope = new Scope(metaField.getMinValue(), metaField.getMaxValue());
      // if there is no true in res, then no record meet the requirement
      RangeFilter rangeFilter = (RangeFilter) ft;
      return rangeFilter.accept(scope);
    } else {
      throw new IllegalArgumentException("Only support set or range");
    }
  }

  /**
   * Check if dataChunk meet the requirements.
   *
   * @param dataChunk       dataChunk
   * @param selectionFilter selecton Filter
   * @return t /f
   */
  private boolean checkDataChunk(ChunkRS dataChunk, OLAPSelectionLayout selectionFilter) {
    if (selectionFilter == null) {
      return true;
    }
    if (selectionFilter.getType().equals(SelectionType.filter)) {
      FieldRS field = dataChunk.getField(selectionFilter.getDimension());
      Filter currentFilter = selectionFilter.getFilter();
      if (currentFilter.getType().equals(FilterType.Range)) {
        DataRangeFieldRS rangeField = (DataRangeFieldRS) field;
        Scope scope = new Scope(rangeField.minKey(), rangeField.maxKey());
        RangeFilter rangeFilter = (RangeFilter) currentFilter;
        return rangeFilter.accept(scope);
      } else {
        // skip check in Set Filter
        return true;
      }
    } else {
      boolean flag = selectionFilter.getType().equals(SelectionType.and);
      for (OLAPSelectionLayout childFilter : selectionFilter.getFields()) {
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
   * Process data chunk.
   * timeRange, selection => groupBY => aggregate on each group.
   *
   * @param metaChunk meta chunk
   * @param dataChunk data chunk
   */
  private void processDataChunk(MetaChunkRS metaChunk, ChunkRS dataChunk) {

    // 1. find all records in dataChunk meet the timeRange and selection requirements.
    // todo: init a selector each time for future optimization.
    OLAPSelector selector = new OLAPSelector();
    BitSet bs = selector.selectRecordsOnDataChunk(this.query.getSelection(), dataChunk);
    // if none of records are selected
    if (bs.nextSetBit(0) == -1) {
      return;
    }

    // 2. run groupBy
    OLAPGroupBy olapGroupBy = new OLAPGroupBy();
    olapGroupBy.groupBy(
        bs,
        this.query.getGroupFields(),
        metaChunk,
        dataChunk,
        query.getGroupFieldsGranularity());

    // 3. run aggregator
    OLAPAggregator olapAgge = new OLAPAggregator();
    for (AggregationLayout aggregation : this.query.getAggregations()) {
      ArrayList<OLAPRet> res =
          olapAgge.process(metaChunk, dataChunk, aggregation, this.projectedSchemaSet,
              olapGroupBy.getMergedGroup());
      result.addAll(res);
    }
  }
}
