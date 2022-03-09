package com.nus.cool.extension.util.reader;

import java.io.FileInputStream;
import java.io.IOException;

import com.nus.cool.core.util.reader.TupleReader;
import com.nus.cool.extension.util.arrow.ArrowBatchIterator;
import com.nus.cool.extension.util.arrow.ArrowRowView;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;

public class ArrowIPCFileTupleReader implements TupleReader {
  
  private final ArrowReader reader;
  private final boolean empty;
  private VectorSchemaRoot root;
  private ArrowBatchIterator itr;
  
  public ArrowIPCFileTupleReader(FileInputStream fileInputStream,
    BufferAllocator allocator,
    CompressionCodec.Factory compressionFactory)
    throws IOException {
    this.reader = new ArrowFileReader(fileInputStream.getChannel(),
      allocator, compressionFactory);
    this.empty = !reader.loadNextBatch();
    if (!this.empty) {
      this.root = this.reader.getVectorSchemaRoot();
      this.itr = new ArrowBatchIterator(root);
    }
  }
  
  public ArrowIPCFileTupleReader(FileInputStream fileInputStream,
  BufferAllocator allocator) throws IOException {
    this.reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);
    this.empty = !reader.loadNextBatch();
    if (!this.empty) {
      this.root = this.reader.getVectorSchemaRoot();
      this.itr = new ArrowBatchIterator(root);
    }
  }


  @Override
  public boolean hasNext() {
    if (empty) return false;
    if (itr.hasNext()) return true;
    try {
      if (!reader.loadNextBatch()) return false;
    } catch (IOException e) {
      System.out.println("Error encountered while loading arrow batch.");
      return false;
    }
    itr = new ArrowBatchIterator(root);
    return itr.hasNext();
  }

  @Override
  public ArrowRowView next() throws IOException {
    ArrowRowView old = itr.next();
    if (old.valid()) return old;
    return hasNext() ? itr.next() : null;
  }

  @Override
  public void close() throws IOException {
    reader.close(true);
  }
}
