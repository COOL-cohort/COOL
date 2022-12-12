package com.nus.cool.core.cohort;

import com.nus.cool.core.cohort.filter.Filter;
import com.nus.cool.core.cohort.filter.FilterType;
import com.nus.cool.core.cohort.olapselect.Aggregation;
import com.nus.cool.core.cohort.olapselect.OLAPAggregation;
import com.nus.cool.core.cohort.olapselect.OLAPSelectionLayout;
import com.nus.cool.core.cohort.olapselect.OLAPSelectionLayout.SelectionType;
import com.nus.cool.core.cohort.olapselect.OLAPSelector;
import com.nus.cool.core.cohort.storage.OlapRet;
import com.nus.cool.core.cohort.storage.Scope;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
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
public class OlapProcessor {

  private OlapQueryLayout query;
  private OLAPSelector selection = new OLAPSelector();
  private List<OlapRet> result = new ArrayList<>();

  @Getter
  private final HashSet<String> projectedSchemaSet = new HashSet<>();

  public OlapProcessor(OlapQueryLayout layout) {
    this.query = layout;
    this.projectedSchemaSet.addAll(this.query.getSchemaSet());
  }

  /**
   * execute iceberg query.
   * timeRange, selection => groupBY => aggregate on each group.
   *
   * @param cube the cube that stores the data we need
   * @return the result of the query
   */
  public List<OlapRet> process(CubeRS cube) throws Exception {

    TableSchema tableschema = cube.getSchema();
    // add this two schema into List

    for (FieldSchema fieldSchema : tableschema.getFields()) {
      if (fieldSchema.getFieldType() == FieldType.UserKey) {
        this.projectedSchemaSet.add(fieldSchema.getName());
      } else if (fieldSchema.getFieldType() == FieldType.ActionTime) {
        this.projectedSchemaSet.add(fieldSchema.getName());
      }
    }

    this.selection.init(this.query);
    for (CubletRS cublet : cube.getCublets()) {
      processCublet(cublet);
    }
    return this.result;
  }

  /**
   * Process each cublet.
   *
   * @param cubletRS cubletRS
   */
  private void processCublet(CubletRS cubletRS) {
    MetaChunkRS metaChunk = cubletRS.getMetaChunk();
    this.selection.getSelection().initSelectionFilter(metaChunk);
    // if this cublet don't meet field requirements
    if (!this.checkMetaChunk(metaChunk, selection.getSelection())) {
      return;
    }

    for (ChunkRS dataChunk : cubletRS.getDataChunks()) {
      if (this.checkDataChunk(dataChunk, selection.getSelection())) {
        this.processDataChunk(metaChunk, dataChunk);
      }
    }
  }

  /**
   * Check if this cublet contains the required field.
   *
   * @param metaChunk       hashMetaFields result
   * @param selectionFilter selection filter
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
  public Boolean checkMetaField(MetaFieldRS metaField, Filter ft) {
    FilterType checkedType = ft.getType();
    if (checkedType.equals(FilterType.Set)) {
      return true;
    } else if (checkedType.equals(FilterType.Range)) {
      Scope scope = new Scope(metaField.getMinValue(), metaField.getMaxValue());
      // if there is no true in res, then no record meet the requirement
      return ft.accept(scope);
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
        Scope scope = new Scope(field.minKey(), field.maxKey());
        return currentFilter.accept(scope);
      }
      // todo: how to check using Set Filter in dataChunk
      return true;

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
   *
   * @param metaChunk meta chunk
   * @param dataChunk data chunk
   */
  private void processDataChunk(MetaChunkRS metaChunk, ChunkRS dataChunk) {
    // 1. find all records in dataChunk meet the timeRange and selection requirements.
    BitSet bs = this.selection.selectRecordsOnDataChunk(dataChunk);
    if (bs.nextSetBit(0) == -1) {
      return;
    }

    // 2. run groupBy
    OLAPAggregation olapAggregator = new OLAPAggregation();
    olapAggregator.groupBy(bs, this.query.getGroupFields(), metaChunk, dataChunk,
        query.getGroupFieldsGranularity());

    for (Aggregation aggregation : this.query.getAggregations()) {
      ArrayList<OlapRet> res = olapAggregator.process(aggregation, this.projectedSchemaSet);
      result.addAll(res);
    }
  }
}
