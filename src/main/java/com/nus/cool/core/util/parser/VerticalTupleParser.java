package com.nus.cool.core.util.parser;

/**
 * Tuple parser for tuple which is separated with a vertical
 * bar(the pipe character, |)
 */
public class VerticalTupleParser implements TupleParser {

    @Override
    public String[] parse(Object tuple) {
        String record = (String) tuple;
        return record.split("\\|", -1);
    }
}
