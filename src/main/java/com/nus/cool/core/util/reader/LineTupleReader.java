package com.nus.cool.core.util.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Read line as tuple from source
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
