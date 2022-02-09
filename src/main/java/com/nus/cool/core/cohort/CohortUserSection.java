/**
 * 
 */
package com.nus.cool.core.cohort;

import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.RLEInputVector;
import com.nus.cool.core.schema.CubeSchema;
import com.nus.cool.core.schema.TableSchema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author qingchao
 *
 */
public class CohortUserSection implements CohortOperator {

    private static Log LOG = LogFactory.getLog(CohortUserSection.class);

    private TableSchema tableSchema;

    private ExtendedCohortSelection sigma;

    private InputVector cohortUsers;

    private int curUser = -1;

    private int totalDataChunks;

    private int totalSkippedDataChunks;

    private int totalUsers;

    private int totalSkippedUsers;

    private final List<Integer> cubletResults = new ArrayList<>();

    public CohortUserSection(ExtendedCohortSelection sigma) {
        this.sigma = checkNotNull(sigma);
    }

    public List<Integer> getCubletResults() {
        return this.cubletResults;
    }

    @Override
    public void close() throws IOException {
        sigma.close();
        LOG.info(String.format("(totalChunks = %d, totalSkippedChunks = %d, totalUsers = %d, totalSkippedUsers = %d)",
                totalDataChunks, totalSkippedDataChunks, totalUsers, totalSkippedUsers));
    }    

	@Override
	public void init(TableSchema tableSchema, InputVector cohortUsers,
                     ExtendedCohortQuery query) {
		LOG.info("Initializing user selection operator ...");
		checkNotNull(query.getBirthSequence());
		this.tableSchema = checkNotNull(tableSchema);
		this.cohortUsers = cohortUsers;
		curUser = -1;
		if (cohortUsers != null && cohortUsers.size() > 0) {
			curUser = cohortUsers.next();
		}
		sigma.init(tableSchema, query);
	}

    @Override
    public void init(TableSchema schema, CohortQuery query) {

    }

    @Override
    public void process(MetaChunkRS metaChunk) {
        LOG.info("Processing metaChunk ...");
        sigma.process(metaChunk);
    }

    @Override
    public boolean isCohortsInCublet() {
        return true;
    }

    @Override
    public void process(ChunkRS chunk) {
        totalDataChunks++;

        sigma.process(chunk);
        if (sigma.isUserActiveChunk() == false) {
            totalSkippedDataChunks++;
            return;
        }

        FieldRS userField = chunk.getField(tableSchema.getUserKeyField());
        
        // Skipping non RLE compressed blocks
        int totalCorruptedUsers = 0;

        if (!(userField.getValueVector() instanceof RLEInputVector)) {
            totalCorruptedUsers++;
            return;
        }

        RLEInputVector userInput = (RLEInputVector) userField.getValueVector();
        RLEInputVector.Block userBlock = new RLEInputVector.Block();
        InputVector userKey = userField.getKeyVector();

		// TODO: later will dynamically determine the scan of cohort users:
		// either do a sequential scan or use the index

		while (userInput.hasNext()) {

			userInput.nextBlock(userBlock); // Next user RLE block

			// Find a new user
			totalUsers++;
			int beg = userBlock.off;
			int end = userBlock.off + userBlock.len;
			
			if (this.cohortUsers != null) {
				if (curUser != userKey.get(userBlock.value) && curUser >= 0)
					continue;
				if (cohortUsers.hasNext())
					curUser = cohortUsers.next();
				else return;
			}

			ExtendedCohort cohort = sigma.selectUser(beg, end);

			if (cohort == null) {
				totalSkippedUsers++;
				continue;
			}
			
			int uid = userKey.get(userBlock.value);
			cubletResults.add(uid);
		}

		if (totalCorruptedUsers > 0)
			LOG.info("Total corrupted users: " + totalCorruptedUsers + " " + totalDataChunks);
	}

}
