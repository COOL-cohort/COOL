package com.nus.cool.core.util.parser;

/**
 * Tuple parser for tuple which is separated with a comma.
 */
public class CsvTupleParser implements TupleParser {

    @Override
    public String[] parse(Object tuple) {
        String record = (String) tuple;
        return record.split(",", -1);
    }
}
