package com.nus.cool.core.cohort.refactor;

import com.nus.cool.core.cohort.refactor.olapSelect.Aggregation;
import com.nus.cool.core.cohort.refactor.olapSelect.olapSelector;
import com.nus.cool.core.cohort.refactor.storage.OlapRet;
import com.nus.cool.core.iceberg.query.IcebergAggregation;
import com.nus.cool.core.iceberg.query.IcebergSelection.TimeBitSet;
import com.nus.cool.core.iceberg.query.SelectionFilter;
import com.nus.cool.core.iceberg.query.SelectionQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import lombok.Data;
import lombok.Getter;

@Data
public class OlapProcessor {

  private TableSchema tableSchema;

  private OlapQueryLayout query;
  private final String dataSource;
  private olapSelector selection = new olapSelector();

  @Getter
  private final OlapRet result = new OlapRet();

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
  public OlapRet process(CubeRS cube) throws  Exception{

    this.tableSchema = cube.getTableSchema();
    this.selection.init(this.tableSchema, this.query);

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
    if (!this.checkMetaChunk(metaChunk)) {
      return;
    }

    for (ChunkRS dataChunk : cubletRS.getDataChunks()) {
      this.processDataChunk(metaChunk, dataChunk);
    }
  }


  /**
   * Check if this cublet contains the required field.
   * @param metaChunk hashMetaFields result
   * @return true: this metaChunk is valid, false: this metaChunk is invalid.
   */
  private Boolean checkMetaChunk(MetaChunkRS metaChunk){
    return this.selection.process(metaChunk);
  }



  /**
   * Process data chunk
   * @param metaChunk meta chunk
   * @param dataChunk data chunk
   */
  private void processDataChunk(MetaChunkRS metaChunk, ChunkRS dataChunk){
    // 1. find all records in dataChunk meet the timeRange and selection requirements.
    ArrayList<TimeBitSet> map = this.selection.process(dataChunk);
    if (map == null) {
      return;
    }
    // 2. for each time range, run aggregation
    for (TimeBitSet timeBitSet : map) {
      String timeRange = timeBitSet.getTimeRange();
      BitSet bs = timeBitSet.getMatchedRecords();

      // 2. run groupBy
      IcebergAggregation icebergAggregation = new IcebergAggregation();
      icebergAggregation.groupBy(bs, this.query.getGroupFields(), metaChunk, dataChunk,
          timeRange, query.getGroupFields_granularity());
      for (Aggregation aggregation : this.query.getAggregations()) {
        List<BaseResult> res = icebergAggregation.process(aggregation);
        result.addAll(res);
      }
    }
  }








}
