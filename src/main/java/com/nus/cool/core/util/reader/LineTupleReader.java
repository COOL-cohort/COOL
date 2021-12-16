/*
 * Copyright 2020 Cool Squad Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.core.util.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Read line as tuple from source
 *
 * @author zhongle
 * @version 0.1
 * @since 0.1
 */
public class LineTupleReader implements TupleReader {

  private String line;

  private BufferedReader reader;

  public LineTupleReader(File in) throws IOException {
    this.reader = new BufferedReader(new FileReader(in));
    this.line = this.reader.readLine();
  }

  @Override
  public boolean hasNext() {
    return this.line != null;
  }

  @Override
  public Object next() throws IOException {
    String old = line;
    this.line = this.reader.readLine();
    return old;
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }
}
