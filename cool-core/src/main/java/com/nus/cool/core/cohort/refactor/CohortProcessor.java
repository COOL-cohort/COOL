package com.nus.cool.core.cohort.refactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.nus.cool.core.cohort.refactor.ageselect.AgeSelection;
import com.nus.cool.core.cohort.refactor.birthselect.BirthSelection;
import com.nus.cool.core.cohort.refactor.cohortselect.CohortSelector;
import com.nus.cool.core.cohort.refactor.filter.Filter;
import com.nus.cool.core.cohort.refactor.storage.CohortRSStr;
import com.nus.cool.core.cohort.refactor.storage.CohortRet;
import com.nus.cool.core.cohort.refactor.storage.CohortWSStr;
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
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

/**
 * Cohort Query Processing Engine.
 */
public class CohortProcessor {

  private final AgeSelection ageSelector;

  private final ValueSelection valueSelector;

  private final CohortSelector cohortSelector;

  private final BirthSelection birthSelector;

  private final CohortQueryLayout layout;

  @Getter
  private final String dataSource;
  @Getter
  private final CohortRet result;
  private final HashSet<String> projectedSchemaSet;
  // initialize cohort result write store
  private final HashMap<String, CohortWSStr> cohortUserMapper = new HashMap<>();
  private ProjectedTuple tuple;
  private String userIdSchema;
  private String actionTimeSchema;
  @Getter
  private HashSet<String> previousCohortUsers = new HashSet<>();

  /**
   * Constructor.
   *
   * @param layout query layout
   */
  public CohortProcessor(CohortQueryLayout layout) {
    this.layout = layout;
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

      // record result user id list
      for (Map.Entry<String, List<String>> ele : this.result.getCohortToUserIdList().entrySet()) {
        String cohortName = ele.getKey();
        List<String> users = ele.getValue();
        if (!this.cohortUserMapper.containsKey(cohortName)) {
          this.cohortUserMapper.put(cohortName, new CohortWSStr());
        }
        this.cohortUserMapper.get(cohortName).addCubletResults(users);
      }
      this.result.clearUserIds();
    }
    return this.result;
  }

  /**
   * Persist cohort file .cohort to output disk to the same level with the .dz
   * file.
   * E,g. ../CubeRepo/health_raw/v00000012.
   *
   * @param outputDir the output file path
   * @return The cohort result storage path
   * @throws IOException IOException
   */
  public String persistCohort(String outputDir) throws IOException {

    // 1. create folder named "cohort" under the current version
    SimpleDateFormat formatter = new SimpleDateFormat("dd_MM_yyyy_HH_mm_ss");
    Date date = new Date();
    String timeStr = formatter.format(date);

    File cohortRes = new File(outputDir, "cohort/query_" + timeStr);
    if (!cohortRes.getParentFile().exists()) {
      cohortRes.getParentFile().mkdir();
    }
    if (!cohortRes.exists()) {
      cohortRes.mkdir();
    }

    // 2.store cohort result
    CohortResultLayout cohortJsonContent = new CohortResultLayout();
    cohortJsonContent.setCohortQuery(this.layout);

    for (Map.Entry<String, CohortWSStr> ele : this.cohortUserMapper.entrySet()) {
      String cohortName = ele.getKey();
      String fileName = cohortName + ".cohort";
      int cohortSize = ele.getValue().getNumUsers();
      File cohortResFile = new File(cohortRes.toString(), fileName);
      DataOutputStream out = new DataOutputStream(new FileOutputStream(cohortResFile));
      ele.getValue().writeTo(out);
      // update info
      cohortJsonContent.addOneCohortRes(fileName, cohortName, cohortSize);
    }

    // 3. store the json file with cohort result, and original query.json
    ObjectMapper mapper = new ObjectMapper();
    String cohortJson = Paths.get(cohortRes.toString(), "query_res.json").toString();
    mapper.writeValue(new File(cohortJson), cohortJsonContent);

    return cohortRes.toString();
  }

  /**
   * Persist cohort to output disk, cohort is named cohortName.cohort, e,g.
   * "1980-1990.cohort".
   * Where 1980-1990 is the cohortName in our cohort query for health-raw dataset.
   *
   * @param cohortPath the path to store the previous stored cohort.
   */
  public void readExistingCohort(String cohortPath) throws IOException {
    CohortRSStr crs = new CohortRSStr(StandardCharsets.UTF_8);

    File file = new File(cohortPath);
    File[] fs = file.listFiles();
    if (fs == null) {
      return;
    }
    for (File f : fs) {
      int pointIndex = f.getName().lastIndexOf(".");
      if (pointIndex == -1) {
        continue;
      }
      String extension = f.getName().substring(pointIndex);
      if (!f.isDirectory() && extension.equals(".cohort")) {
        crs.readFrom(Files.map(f).order(ByteOrder.nativeOrder()));
        this.previousCohortUsers.addAll(crs.getUsers());
      }
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
   * @param chunk     dataChunk
   * @param metaChunk metaChunk
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

    // only process the user in previous cohort.
    if (!this.previousCohortUsers.isEmpty() && !previousCohortUsers.contains(userId)) {
      return;
    }

    LocalDateTime actionTime = DateUtils.daysSinceEpoch(
        (int) tuple.getValueBySchema(this.actionTimeSchema));
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
   * the Interface to get selectedUser in CohortProcessing.
   *
   * @return a set of userId
   */
  Set<String> getSelectedUser() {
    return this.birthSelector.getAcceptedUsers();
  }

}