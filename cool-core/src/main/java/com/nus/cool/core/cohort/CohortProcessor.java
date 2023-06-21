package com.nus.cool.core.cohort;

import com.google.common.io.Files;
import com.nus.cool.core.cohort.ageselect.AgeSelection;
import com.nus.cool.core.cohort.birthselect.BirthSelection;
import com.nus.cool.core.cohort.cohortselect.CohortSelector;
import com.nus.cool.core.cohort.storage.CohortRSStr;
import com.nus.cool.core.cohort.storage.CohortRet;
import com.nus.cool.core.cohort.storage.ProjectedTuple;
import com.nus.cool.core.cohort.storage.RetUnit;
import com.nus.cool.core.cohort.utils.DateUtils;
import com.nus.cool.core.cohort.valueselect.ValueSelection;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;

/**
 * Cohort Query Processing Engine.
 */
public class CohortProcessor {
  private AgeSelection ageSelector;

  private ValueSelection valueSelector;

  private CohortSelector cohortSelector;

  private BirthSelection birthSelector;

  @Getter
  private final String dataSource;

  @Getter
  private final String inputCohort;

  private CohortRet result;

  private final Set<String> projectedSchemaSet;
  // initialize cohort result write store

  private ProjectedTuple tuple;

  private String userIdSchema;

  private String actionTimeSchema;

  private Set<String> previousCohortUsers = new HashSet<>();

  /**
   * Constructor.
   *
   * @param layout query layout
   */
  public CohortProcessor(CohortQueryLayout layout) {
    // get birth selector
    if (layout.getBirthSelectionLayout() != null) {
      this.birthSelector = layout.getBirthSelectionLayout().generate();
    }
    // get cohort selector
    this.cohortSelector = layout.getCohortSelectionLayout().generate();
    this.result = new CohortRet();
    // get age selector
    if (layout.getAgeSelectionLayout() != null) {
      this.ageSelector = layout.getAgeSelectionLayout().generate();
      this.result = new CohortRet(layout.getAgeSelectionLayout());
    }
    // get value selector
    if (layout.getValueSelectionLayout() != null) {
      this.valueSelector = layout.getValueSelectionLayout().generate();
    }
    this.projectedSchemaSet = layout.getSchemaSet();
    this.dataSource = layout.getDataSource();
    this.inputCohort = layout.getInputCohort();
  }

  public int getInputCohortSize() {
    return previousCohortUsers.size();
  }

  /**
   * Public interface, Scan whole table and return CohortResult.
   *
   * @param cube Cube
   * @return CohortRet
   */
  public CohortRet process(CubeRS cube) throws IOException {
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
    for (CubletRS cublet : cube.getCublets()) {
      processCublet(cublet);

    }
    return this.result;
  }

  /**
   * Read one cohort from the results of a previous query,
   * cohort is named cohortName.cohort, e,g. "1980-1990.cohort".
   * Where 1980-1990 is the cohortName in our cohort query for health-raw dataset.
   *
   * @param cohortName name of the previous stored cohort.
   * @param cohortFolderPath the path to store the previous stored cohort.
   */
  public void readOneCohort(String cohortName, String cohortFolderPath) throws IOException {
    
    // check folder
    File folder = new File(cohortFolderPath);
    File[] fs = folder.listFiles();
    if (fs == null) {
      return;
    }
    File cohortFile = new File(folder, cohortName + ".cohort");
    readOneCohort(cohortFile);
  }

  /**
   * Read a cohort from the results of a previous query.
   * cohort is named cohortName.cohort, e,g. "all.cohort".
   *
   * @param cohortPath the path to store a stored cohort.
   */
  public void readOneCohort(String cohortPath) throws IOException {
    File file = new File(cohortPath);
    readOneCohort(file);
  }

  /**
   * Read a cohort from the results of a previous query.
   * cohort is named cohortName.cohort, e,g. "all.cohort".
   *
   * @param cohortFile the File to store a stored cohort.
   */
  public void readOneCohort(File cohortFile) throws IOException {
    CohortRSStr crs = new CohortRSStr(StandardCharsets.UTF_8);

    if (!cohortFile.exists()) {
      throw new IOException("[*] cohort file is not found. Cohort path: " + cohortFile.getPath());
    }
    int pointIndex = cohortFile.getName().lastIndexOf(".");
    if (pointIndex == -1) {
      throw new IOException("[*] Not a valid cohort file which should end with '.cohort'. "
          + "Cohort path: " + cohortFile.getPath());
    }
    String extension = cohortFile.getName().substring(pointIndex);
    if (!cohortFile.isDirectory() && extension.equals(".cohort")) {
      crs.readFrom(Files.map(cohortFile));
      this.previousCohortUsers.addAll(crs.getUsers());
    }
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
    if (this.skipMetaChunk(metaChunk)) {
      return;
    }

    // Now start to pass the DataChunk
    for (ChunkRS chunk : cublet.getDataChunks()) {
      if (this.skipDataChunk(chunk)) {
        continue;
      }
      this.processDataChunk(chunk, metaChunk);
    }
  }

  /**
   * In this section, we load the tuple which is an inner property.
   * We left the process logic in processTuple function.
   *
   * @param chunk     dataChunk
   * @param metaChunk metaChunk
   */
  private void processDataChunk(ChunkRS chunk, MetaChunkRS metaChunk) {
    for (int i = 0; i < chunk.getRecords(); i++) {
      // load data into tuple
      for (String schema : this.projectedSchemaSet) {
        FieldValue value = chunk.getField(schema).getValueByIndex(i);
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
    int userGlobalID = tuple.getValueBySchema(this.userIdSchema).getInt();
    MetaFieldRS userMetaField = metaChunk.getMetaField(this.userIdSchema);
    // String userId = userMetaField.getString(userGlobalID);
    String userId = userMetaField.get(userGlobalID).map(FieldValue::getString).orElse("");

    // only process the user in previous cohort.
    // todo: nl why?
    if (!this.previousCohortUsers.isEmpty() && !previousCohortUsers.contains(userId)) {
      return;
    }

    LocalDateTime actionTime =
        DateUtils.secondsSinceEpoch(tuple.getValueBySchema(this.actionTimeSchema).getInt());
    // check whether its birthEvent is selected
    if (!this.birthSelector.isUserSelected(userId)) {
      boolean selected = this.birthSelector.selectEvent(userId, actionTime, this.tuple);
      if (!selected || !this.birthSelector.isUserSelected(userId)) {
        // if birthEvent is not selected, or birth event selected but user not born yet.
        return;
      } 
    }
    // user is born
    // extract the cohort this tuple belong to
    String cohortName = this.cohortSelector.selectCohort(this.tuple, metaChunk);
    if (cohortName == null) {
      // cohort is outofrange
      return;
    }
    this.result.addUserid(cohortName, userId);

    if (this.ageSelector != null && this.valueSelector != null) {
      // do time_diff to generate age / get the BirthEvent Date
      LocalDateTime birthTime = this.birthSelector.getUserBirthEventDate(userId);
      
      assert birthTime != null : "birthTime null";
      assert actionTime != null : "actionTime null";
      int age = this.ageSelector.generateAge(birthTime, actionTime);
      if (age == AgeSelection.DefaultNullAge) {
        // age is outofrange
        return;
      }
      if (!this.valueSelector.isSelected(this.tuple)) {
        // value outofrange
        return;
      }
      // Pass all above filter, we can store value into CohortRet
      // get the temporary result for this CohortGroup and this age
      RetUnit ret = this.result.getByAge(cohortName, age);

      this.valueSelector.getAggregateFunc().calculate(ret, tuple);
    }
  }

  /**
   * Check if this cublet contains the required field.
   *
   * @param metaChunk hashMetaFields result
   * @return true: this metaChunk is skipped , false otherwise.
   */
  private Boolean skipMetaChunk(MetaChunkRS metaChunk) {
    // 1. check birth selection
    // if the metaChunk contains all birth filter's accept value, then the metaChunk
    // is valid.
    // 2. check birth selection
    // 3. check value Selector,
    return birthSelector.maybeSkipMetaChunk(metaChunk)
      && cohortSelector.maybeSkipMetaChunk(metaChunk)
      && valueSelector.maybeSkipMetaChunk(metaChunk);
  }


  /***
   * Now is not implemented.
   */
  public Boolean skipDataChunk(ChunkRS chunk) {
    return false;
  }

  /**
   * When new Cublet is coming, transfer the set value in Filter to GlobalID.
   *
   * @param metaChunkRS neraChunkRS
   */
  private void filterInit(MetaChunkRS metaChunkRS) {
    // init birthSelector
    this.birthSelector.loadMetaInfo(metaChunkRS);

    // init cohort
    this.cohortSelector.loadMetaInfo(metaChunkRS);

    // value age
    if (this.valueSelector != null) {
      valueSelector.loadMetaInfo(metaChunkRS);
    }
  }
}
