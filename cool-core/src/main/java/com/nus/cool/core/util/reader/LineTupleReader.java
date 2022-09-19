/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.nus.cool.core.util.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Read line as tuple from source.
 * LineTupleReader is a high-level data loader that read contents form
 * FileReader
 * LineTupleReader could test whether it has finished reading the file
 * and close the FileReader
 */
public class LineTupleReader implements TupleReader {

  /**
   * The line to store the loaded data.
   */
  private String line;

  /**
   * The reader to read the content of files.
   */
  private BufferedReader reader;

  public LineTupleReader(File in) throws IOException {
    this.reader = new BufferedReader(new FileReader(in));
    this.line = this.reader.readLine();
  }

  /**
   * check whether the reader finishes reading.
   *
   * @return 1 denotes the reader still needs to read and
   *         0 denoets there is nothing left to read.
   */
  @Override
  public boolean hasNext() {
    return this.line != null;
  }

  /**
   * get the next line of file.
   *
   * @return the original line
   */
  @Override
  public Object next() throws IOException {
    String old = line;
    this.line = this.reader.readLine();
    return old;
  }

  /**
   * Close the read.
   */
  @Override
  public void close() throws IOException {
    this.reader.close();
  }
}
