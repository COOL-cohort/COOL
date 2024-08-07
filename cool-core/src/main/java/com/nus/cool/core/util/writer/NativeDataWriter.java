package com.nus.cool.core.util.writer;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.nus.cool.core.field.FieldValue;
import com.nus.cool.core.io.writestore.DataChunkWS;
import com.nus.cool.core.io.writestore.MetaChunkWS;
import com.nus.cool.core.schema.TableSchema;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native data writer writes a set of records in cool storage format.
 */
@RequiredArgsConstructor
public class NativeDataWriter implements DataWriter {

  static final Logger logger = LoggerFactory.getLogger(NativeDataWriter.class);

  @NotNull
  private final TableSchema tableSchema;

  @NotNull
  private final File outputDir;

  @NotNull
  private final long chunkSize;

  @NotNull
  private final long cubletSize;

  // states.

  private boolean initalized = false;

  private boolean finished = false;

  // below are variables describing the dataset building context

  // initialzied once
  private int userKeyIndex;

  private MetaChunkWS metaChunk;

  // updated as building progress
  private int offset = 0;

  private int tupleCount = Integer.MAX_VALUE;

  private FieldValue lastUser = null;

  // record the Header offset of each chunk
  private final List<Integer> chunkHeaderOffsets = Lists.newArrayList();

  private DataChunkWS dataChunk;

  private DataOutputStream out = null;

  @Override
  public boolean initialize() throws IOException {
    if (initalized) {
      return true;
    }
    this.userKeyIndex = tableSchema.getUserKeyFieldIdx();

    // when there is no user key, using any field for the additional condition on
    // chunk switch is ok.
    if (this.userKeyIndex == -1) {
      this.userKeyIndex = 0;
    }
    // cublet
    // create metaChunk instance, default offset to be 0, update offset when write
    // later.
    this.metaChunk = MetaChunkWS.newMetaChunkWS(this.tableSchema, 0);
    this.out = newCublet();
    // chunk
    this.tupleCount = 0;
    this.offset = 0;
    // create dataChunk instance
    this.dataChunk = DataChunkWS.newDataChunk(this.tableSchema, this.metaChunk.getMetaFields(),
        this.offset);
    this.initalized = true;
    return true;
  }

  /**
   * Update current offset, begin write to a new dataChunk.
   */
  private void finishChunk() throws IOException {
    offset += dataChunk.writeTo(out);
    // record the header offset in chunkHeaderOffsets.
    chunkHeaderOffsets.add(offset - Ints.BYTES);
  }

  /**
   * If chunkSize is greater 65536 or meet last user.
   *
   * @param curUser current user id
   * @return is chunk full or is last user
   */
  private boolean maybeSwitchChunk(FieldValue curUser) throws IOException {
    if ((tupleCount < chunkSize) || (curUser.checkEqual(lastUser))) {
      return false;
    }
    finishChunk();
    // create a new data chunk, init tuple Count
    logger.debug("newDataChunk: offset=" + offset);
    dataChunk = DataChunkWS.newDataChunk(tableSchema, metaChunk.getMetaFields(), offset);
    tupleCount = 0;
    return true;
  }

  /**
   * Write metaChunk lastly.
   */
  private void finishCublet() throws IOException {
    // write metaChunk begin from current offset.
    this.metaChunk.updateBeginOffset(this.offset);
    this.metaChunk.complete();
    offset += this.metaChunk.writeTo(out);
    this.metaChunk.cleanForNextCublet();
    // record header offset
    chunkHeaderOffsets.add(offset - Ints.BYTES);
    // 1. write number of chunks
    out.writeInt(chunkHeaderOffsets.size());
    // 2. write header of each chunk
    for (int chunkOff : chunkHeaderOffsets) {
      out.writeInt(chunkOff);
    }
    // 3. write the header offset.
    out.writeInt(offset);
    // 4. flush after writing whole Cublet.
    out.flush();
    out.close();

    this.offset = 0;
  }

  /**
   * Create I/O stream to write data.
   *
   * @return DataOutputStream
   */
  private DataOutputStream newCublet() throws IOException {
    String fileName = Long.toHexString(System.currentTimeMillis()) + ".dz";
    System.out.println("[*] A new cublet " + fileName + " is created!");
    File cublet = new File(outputDir, fileName);
    DataOutputStream out = new DataOutputStream(new FileOutputStream(cublet));
    chunkHeaderOffsets.clear();
    return out;
  }

  /**
   * Switch a new cublet File once meet 1GB.
   */
  private boolean maybeSwitchCublet() throws IOException {
    if (offset < cubletSize) {
      return false;
    }
    finishCublet();
    logger.debug("switching cublet...");

    out = newCublet();

    return true;
  }

  @Override
  public boolean add(FieldValue[] tuple) throws IOException {
    FieldValue curUser = tuple[userKeyIndex];
    if (lastUser == null) {
      lastUser = curUser;
    }
    // start a new chunk
    if (maybeSwitchChunk(curUser)) {
      if (maybeSwitchCublet()) {
        // create a new data chunk with offset 0
        this.dataChunk = DataChunkWS.newDataChunk(
          this.tableSchema, this.metaChunk.getMetaFields(), 0);
      }
    }
    lastUser = curUser;
    // update metachunk / metafield
    metaChunk.put(tuple);
    dataChunk.put(tuple);
    // update data chunk
    tupleCount++;
    return true;
  }

  private void generateCubeMeta() throws IOException {
    if (!finished) {
      return;
    }
    String fileName = "cubemeta";
    File cubemeta = new File(outputDir, fileName);
    DataOutputStream out = new DataOutputStream(new FileOutputStream(cubemeta));
    offset = 0;
    metaChunk.writeCubeMeta(out);
    out.flush();
    out.close();
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      return;
    }
    finishChunk();
    finishCublet();
    finished = true;
    generateCubeMeta();
  }

  @Override
  public void close() throws IOException {
    // force close the out stream
    if (!finished) {
      finish();
    }
  }
}
