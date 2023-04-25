package com.nus.cool.core.io.compression;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Abstract the byte array allocated for compression as a source for other operation.
 * Hiding the capacity and exposing its actual size.
 */
@AllArgsConstructor
@Data
public class CompressorOutput {
  
  private byte[] buf = null;
  
  private int len = 0;
  
  private CompressorOutput() {}

  private static CompressorOutput emptyInstance = new CompressorOutput();

  public CompressorOutput(int capacity) {
    this.buf = new byte[capacity];
  } 

  public CompressorOutput(byte[] buf) {
    this.buf = buf;
    this.len = buf.length;
  } 
  
  public static CompressorOutput emptyCompressorOutput() {
    return emptyInstance;
  }

  public int capacity() {
    return buf.length;
  }
}
