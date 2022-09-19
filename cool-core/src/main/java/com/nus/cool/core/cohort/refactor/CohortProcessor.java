package com.nus.cool.core.cohort.refactor;

import com.nus.cool.core.cohort.refactor.storage.CohortWS;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cool.core.cohort.refactor.ageselect.AgeSelection;
import com.nus.cool.core.cohort.refactor.birthselect.BirthSelection;
import com.nus.cool.core.cohort.refactor.cohortselect.CohortSelector;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.cohort.refactor.storage.ProjectedTuple;
import com.nus.cool.core.cohort.refactor.storage.RetUnit;
import com.nus.cool.core.cohort.refactor.utils.DateUtils;
import com.nus.cool.core.cohort.refactor.valueselect.ValueSelection;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import lombok.Getter;

/**
 * Cohort Query Processing Engine.
 */
public class CohortProcessor {

  private final AgeSelection ageSelector;

  private final ValueSelection valueSelector;

  private final CohortSelector cohortSelector;

  private final BirthSelection birthSelector;

  @Getter
  private final String dataSource;

  private ProjectedTuple tuple;

  @Getter
  private final CohortRet result;

  private String userIdSchema;

  private String actionTimeSchema;

  private final HashSet<String> projectedSchemaSet;

  /**
   * Constructor.
   *
   * @param layout query layout
   */
  public CohortProcessor(CohortQueryLayout layout) {

    this.ageSelector = layout.getAgetSelectionLayout().generate();
    this.birthSelector = layout.getBirthSelectionLayout().generate();
    this.cohortSelector = layout.getCohortSelectionLayout().generate();
    this.valueSelector = layout.getValueSelectionLayout().generate();

    this.projectedSchemaSet = layout.getSchemaSet();
    this.dataSource = layout.getDataSource();
    this.result = new CohortRet(layout.getAgetSelectionLayout());
  }

  /**
   * Public interface, Scan whole table and return CohortResult.
   *
   * @param cube Cube
   * @return CohortRet
   */
  public CohortRet process(CubeRS cube, String outputDir) throws IOException {
    // initialize the UserId and KeyId
    TableSchema tableschema = cube.getSchema();
    for (FieldSchema fieldSchema : tableschema.getFields()) {
      if (fieldSchema.getFieldType() == FieldType.UserKey) {
        this.userIdSchema = fieldSchema.getName();
      } else if (fieldSchema.getFieldType() == FieldType.ActionTime) {
        this.actionTimeSchema = fieldSchema.getName();
      }
    }
    // add this two schema into List
    this.projectedSchemaSet.add(this.userIdSchema);
    this.projectedSchemaSet.add(this.actionTimeSchema);
    this.tuple = new ProjectedTuple(this.projectedSchemaSet);

    // initialize cohort result write store
    HashMap<String, CohortWS > CohortUserMapper = new HashMap<>();

    for (CubletRS cublet : cube.getCublets()) {
      processCublet(cublet);

      // record result user id list
      for (Map.Entry<String, List<String>> ele: this.result.getCohortToUserIdList().entrySet()){

        String cohortName = ele.getKey();
        List<String> users = ele.getValue();
        if (!CohortUserMapper.containsKey(cohortName)){
          CohortUserMapper.put(cohortName, new CohortWS(cube.getCublets().size()));
        }

        CohortUserMapper.get(cohortName).addCubletResults(users);
      }
      this.result.ClearUserIds();
    }

    // iterate all the write-store in CohortUserMapper, and sync it to disk
    for (Map.Entry<String, CohortWS > ele: CohortUserMapper.entrySet()){

      String fileName = ele.getKey();
      File cubemeta = new File(outputDir, fileName);
      DataOutputStream out = new DataOutputStream(
          new FileOutputStream(cubemeta));
      ele.getValue().writeTo(out);
    }

    return this.result;
  }

  /**
   * Process one Cublet.
   *
   * @param cublet cublet
   */
  private void processCublet(CubletRS cublet) {
    MetaChunkRS metaChunk = cublet.getMetaChunk();
    // transfer values in SetFilter into GloablId
    this.filterInit(metaChunk);

    // use MetaChunk to skip Cublet
    if (!this.checkMetaChunk(metaChunk)) {
      return;
    }

    // Now start to pass the DataChunk
    for (ChunkRS chunk : cublet.getDataChunks()) {
      if (this.checkDataChunk(chunk)) {
        this.processDataChunk(chunk, metaChunk);
      }
    }
  }

  /**
   * In this section, we load the tuple which is an inner property.
   * We left the process logic in processTuple function.
   *
<<<<<<< HEAD
   * @param chunk     dataChunk
   * @param metaChunk metaChunk
=======
   * @param chunk              dataChunk
   * @param metaChunk          metaChunk
>>>>>>> c906bf6 (Store cohort result after cohort-processing)
   */
    private void processDataChunk(ChunkRS chunk, MetaChunkRS metaChunk) {
      for (int i = 0; i < chunk.getRecords(); i++) {
        // load data into tuple
        for (String schema : this.projectedSchemaSet) {
          int value = chunk.getField(schema).getValueByIndex(i);
          this.tuple.loadAttr(value, schema);
        }

        this.processTuple(metaChunk);
      }
    }


  /**
   * process the inner tuple.
   */
  private void processTuple(MetaChunkRS metaChunk) {
    // For One Tuple, we firstly get the userId, and ActionTime
    int userGlobalID = (int) tuple.getValueBySchema(this.userIdSchema);
    MetaFieldRS userMetaField = metaChunk.getMetaField(this.userIdSchema);
    String userId = userMetaField.getString(userGlobalID);

    LocalDateTime actionTime =
        DateUtils.daysSinceEpoch((int) tuple.getValueBySchema(this.actionTimeSchema));
    // check whether its birthEvent is selected
    if (!this.birthSelector.isUserSelected(userId)) {
      // if birthEvent is not selected
      this.birthSelector.selectEvent(userId, actionTime, this.tuple);
    } else {
      // the birthEvent is selected
      // do time_diff to generate age / get the BirthEvent Date
      LocalDateTime birthTime = this.birthSelector.getUserBirthEventDate(userId);
      int age = this.ageSelector.generateAge(birthTime, actionTime);
      if (age == AgeSelection.DefaultNullAge) {
        // age is outofrange
        return;
      }
      // extract the cohort this tuple belong to
      String cohortName = this.cohortSelector.selectCohort(this.tuple, metaChunk);
      if (cohortName == null) {
        // cohort is outofrange
        return;
      }
      if (!this.valueSelector.isSelected(this.tuple)) {
        // value outofrange
        return;
      }
      // Pass all above filter, we can store value into CohortRet
      // get the temporay result for this CohortGroup and this age
      RetUnit ret = this.result.getByAge(cohortName, age);
      this.result.addUserid(cohortName, userId);
      this.valueSelector.getAggregateFunc().calculate(ret, tuple);
    }
  }

  /**
   * Check if this cublet contains the required field.
   *
   * @param metaChunk hashMetaFields result
   * @return true: this metaChunk is valid, false: this metaChunk is invalid.
   */
  private Boolean checkMetaChunk(MetaChunkRS metaChunk) {

    // 1. check birth selection
    // if the metaChunk contains all birth filter's accept value, then the metaChunk
    // is valid.
    if (this.birthSelector.getBirthEvents() == null) {
      return true;
    }

    for (Filter filter : this.birthSelector.getFilterList()) {
      String checkedSchema = filter.getFilterSchema();
      MetaFieldRS metaField = metaChunk.getMetaField(checkedSchema);
      if (this.checkMetaField(metaField, filter)) {
        return true;
      }
    }

    // 2. check birth selection
    Filter cohortFilter = this.cohortSelector.getFilter();
    String checkedSchema = cohortFilter.getFilterSchema();
    MetaFieldRS metaField = metaChunk.getMetaField(checkedSchema);
    if (this.checkMetaField(metaField, cohortFilter)) {
      return true;
    }

    // 3. check value Selector,
    for (Filter ft : this.valueSelector.getFilterList()) {
      String valueSchema = ft.getFilterSchema();
      MetaFieldRS valueMetaField = metaChunk.getMetaField(valueSchema);
      if (this.checkMetaField(valueMetaField, ft)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Now is not implemented.
   */
  public Boolean checkMetaField(MetaFieldRS metaField, Filter ft) {
    return true;
  }

  /***
   * Now is not implemented.
   */
  public Boolean checkDataChunk(ChunkRS chunk) {
    return true;
  }

  /**
   * When new Cublet is coming, transfer the set value in Filter to GlobalID.
   *
   * @param metaChunkRS neraChunkRS
   */
  private void filterInit(MetaChunkRS metaChunkRS) {
    // init birthSelector
    for (Filter filter : this.birthSelector.getFilterList()) {
      filter.loadMetaInfo(metaChunkRS);
    }

    // init cohort
    this.cohortSelector.getFilter().loadMetaInfo(metaChunkRS);

    // value age
    for (Filter filter : this.valueSelector.getFilterList()) {
      filter.loadMetaInfo(metaChunkRS);
    }
  }

  /**
   * Read from json file and create a instance of CohortProcessor.
   *
   * @param in File
   * @return instance of file
   * @throws IOException IOException
   */
  public static CohortProcessor readFromJson(File in) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    CohortProcessor instance = mapper.readValue(in, CohortProcessor.class);
    return instance;
  }

  public static CohortProcessor readFromJson(String path) throws IOException {
    return readFromJson(new File(path));
  }

}