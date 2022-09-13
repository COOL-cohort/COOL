/**
 * 
 */

package com.nus.cool.core.cohort.funnel;

import static com.google.common.base.Preconditions.checkNotNull;

import com.nus.cool.core.cohort.BirthSequence;
import com.nus.cool.core.cohort.CohortOperator;
import com.nus.cool.core.cohort.CohortQuery;
import com.nus.cool.core.cohort.ExtendedCohort;
import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.ExtendedCohortSelection;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.RLEInputVector;
import com.nus.cool.core.schema.TableSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class FunnelProcess implements CohortOperator {

  private static Log LOG = LogFactory.getLog(FunnelProcess.class);

  private TableSchema tableSchema;

  private List<ExtendedCohortSelection> sigma = new ArrayList<>();

  private InputVector cohortUsers;

  private int curUser = -1;

  private List<ExtendedCohortQuery> cohortQueries = new ArrayList<>();

  private FunnelQuery funnelQuery;

  private int totalDataChunks;

  private int totalSkippedDataChunks;

  private int totalUsers;

  private int totalSkippedUsers;

  MetaChunkRS metaChunk;

  int[] cubletResults;

  int validFunnelStages;

  public FunnelProcess() {
  }

  public Object getCubletResults() {
    return this.cubletResults;
  }

  @Override
  public void close() throws IOException {
    for (ExtendedCohortSelection selection : sigma) {
      selection.close();
    }

    LOG.info(String.format(
        "(totalChunks = %d, totalSkippedChunks = %d, totalUsers = %d, totalSkippedUsers = %d)",
        totalDataChunks, totalSkippedDataChunks, totalUsers, totalSkippedUsers));
  }

  @Override
  public void init(TableSchema tableSchema, InputVector cohortUsers, ExtendedCohortQuery query) {
    throw new UnsupportedOperationException();
  }

  /**
   * Initialize funnel processing.
   * 
   * @param cohortUsers existing cohort
   */
  public void init(TableSchema tableSchema, InputVector cohortUsers, FunnelQuery query) {
    LOG.info("Initializing cohort aggregation operator ...");
    this.tableSchema = checkNotNull(tableSchema);
    this.cohortUsers = cohortUsers;

    curUser = -1;
    if (cohortUsers != null && cohortUsers.size() > 0) {
      curUser = cohortUsers.next();
    }

    this.funnelQuery = query;

    cubletResults = new int[funnelQuery.getStages().size()];
    for (int i = 0; i < cubletResults.length; i++) {
      cubletResults[i] = 0;
    }

    for (BirthSequence sequence : funnelQuery.getStages()) {
      // clear cohort fields
      for (BirthSequence.BirthEvent event : sequence.getBirthEvents()) {
        event.getCohortFields().clear();
      }
      ExtendedCohortQuery cohortQuery = new ExtendedCohortQuery();
      cohortQuery.setBirthSequence(sequence);
      cohortQueries.add(cohortQuery);

      ExtendedCohortSelection selection = new ExtendedCohortSelection();
      sigma.add(selection);
      selection.init(tableSchema, cohortQuery);
    }
  }

  @Override
  public void init(TableSchema schema, CohortQuery query) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCohortsInCublet() {
    return true;
  }

  @Override
  public void process(MetaChunkRS metaChunk) {
    LOG.info("Processing metaChunk ...");
    this.metaChunk = metaChunk;
    validFunnelStages = 0;
    for (ExtendedCohortSelection selection : sigma) {
      selection.process(metaChunk);
      if (!selection.isUserActiveCublet()) {
        break;
      }
      validFunnelStages++;
    }
  }

  @Override
  public void process(ChunkRS chunk) {
    totalDataChunks++;

    int validStages = 0;
    for (int i = 0; i < this.validFunnelStages; i++) {
      ExtendedCohortSelection selection = sigma.get(i);
      selection.process(chunk);
      if (!selection.isUserActiveChunk()) {
        break;
      }
      validStages++;
    }

    if (validStages == 0) {
      this.totalSkippedDataChunks++;
      return;
    }

    FieldRS userField = chunk.getField(tableSchema.getUserKeyField());
    if (userField.getValueVector() instanceof RLEInputVector == false) {
      return;
    }

    RLEInputVector userInput = (RLEInputVector) userField.getValueVector();
    RLEInputVector.Block userBlock = new RLEInputVector.Block();
    InputVector userKey = userField.getKeyVector();

    // TODO: later will dynamically determine the scan of cohort users:
    // either do a sequential scan or use the index

    while (userInput.hasNext()) {

      userInput.nextBlock(userBlock); // Next user RLE block

      int beg = userBlock.off;
      int end = userBlock.off + userBlock.len;

      if (this.cohortUsers != null) {
        if (curUser != userKey.get(userBlock.value) && curUser >= 0) {
          continue;
        }
        if (cohortUsers.hasNext()) {
          curUser = cohortUsers.next();
        } else {
          return;
        }
      }

      // Find a new user
      totalUsers++;

      for (int i = 0; i < validStages && beg < end; i++) {
        ExtendedCohort cohort = sigma.get(i).selectUser(beg, end);

        if (cohort == null) {
          break;
        }

        cubletResults[i]++;

        if (funnelQuery.isOrdered()) {
          beg = cohort.getBirthOffset();
        }
      }
    }
  }
}
