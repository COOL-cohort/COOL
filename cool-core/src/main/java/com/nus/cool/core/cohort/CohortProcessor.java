package com.nus.cool.core.cohort;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.nus.cool.core.cohort.ageselect.AgeSelection;
import com.nus.cool.core.cohort.birthselect.BirthSelection;
import com.nus.cool.core.cohort.cohortselect.CohortSelector;
import com.nus.cool.core.cohort.storage.CohortRSStr;
import com.nus.cool.core.cohort.storage.CohortRet;
import com.nus.cool.core.cohort.storage.CohortWSStr;
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
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * Cohort Query Processing Engine.
 */
public class CohortProcessor {
  private String queryName;

  private AgeSelection ageSelector;

  private ValueSelection valueSelector;

  private CohortSelector cohortSelector;

  private BirthSelection birthSelector;

  private final CohortQueryLayout layout;

  @Getter
  private String dataSource;

  @Getter
  private String inputCohort;

  @Getter
  private CohortRet result;

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
    // get age selector
    if (layout.getAgetSelectionLayout() != null) {
      this.ageSelector = layout.getAgetSelectionLayout().generate();
      this.result = new CohortRet(layout.getAgetSelectionLayout());
    }
    // get birth selector
    if (layout.getBirthSelectionLayout() != null) {
      this.birthSelector = layout.getBirthSelectionLayout().generate();
    }
    // get cohort selector
    this.cohortSelector = layout.getCohortSelectionLayout().generate();
    if (this.cohortSelector.selectAll()) {
      this.result = new CohortRet();
    }
    // get value selector
    if (layout.getValueSelectionLayout() != null) {
      this.valueSelector = layout.getValueSelectionLayout().generate();
    }

    this.projectedSchemaSet = layout.getSchemaSet();
    this.dataSource = layout.getDataSource();
    this.queryName = layout.getQueryName();
    this.inputCohort = layout.getInputCohort();
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
   * Persist cohort file .cohort to output disk to the same level with the .dz file.
   * E,g. ../CubeRepo/health_raw/v00000012/cohort/queryName/all.cohort.
   *
   * @param outputDir the output file path
   * @return The cohort result storage path
   * @throws IOException IOException
   */
  public String persistCohort(String outputDir) throws IOException {

    // 1. create folder named "cohort" under the current version
    File cohortRes = new File(outputDir, "cohort/" + queryName);
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
   * Read all cohorts from the results of a previous query,
   * cohort is named cohortName.cohort, e,g. "1980-1990.cohort".
   * Where 1980-1990 is the cohortName in our cohort query for health-raw dataset.
   *
   * @param cohortFolderPath the path to store the previous stored cohort.
   */
  public void readQueryCohorts(String cohortFolderPath) throws IOException {
    CohortRSStr crs = new CohortRSStr(StandardCharsets.UTF_8);

    File file = new File(cohortFolderPath);
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
        crs.readFrom(Files.map(f));
        this.previousCohortUsers.addAll(crs.getUsers());
      }
    }
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
    if (!this.previousCohortUsers.isEmpty() && !previousCohortUsers.contains(userId)) {
      return;
    }

    // TODO: there is an error in the following code
    // Error: A user with only one record will never be birthed.
    // Please check the CohortSelectionTest.java file
    LocalDateTime actionTime =
        DateUtils.daysSinceEpoch(tuple.getValueBySchema(this.actionTimeSchema).getInt());
    // check whether its birthEvent is selected
    if (!this.birthSelector.isUserSelected(userId)) {
      // if birthEvent is not selected
      this.birthSelector.selectEvent(userId, actionTime, this.tuple);
    } else {
      // the birthEvent is selected
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
