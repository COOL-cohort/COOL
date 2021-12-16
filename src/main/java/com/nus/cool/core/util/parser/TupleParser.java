package com.nus.cool.core.util.parser;

/**
 * Interface of parse tuple to array
 */
public interface TupleParser {

    /**
     * Parse tuple to array
     *
     * @param tuple target tuple
     * @return string array
     */
    public String[] parse(Object tuple);
}
