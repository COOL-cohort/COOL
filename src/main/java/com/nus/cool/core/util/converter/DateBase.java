package com.nus.cool.core.util.converter;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * The base date for all date-related storage
 */
public class DateBase {

    /**
     * Date formatter
     */
    public static final DateTimeFormatter FORMATTER;

    /**
     * Reference day
     */
    public static final DateTime BASE;

    static {
        FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
        BASE = FORMATTER.parseDateTime("1970-01-01");
    }
}
