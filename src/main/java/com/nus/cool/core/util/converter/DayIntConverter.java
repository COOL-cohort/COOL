package com.nus.cool.core.util.converter;

import org.joda.time.DateTime;
import org.joda.time.Days;

/**
 * Convert the input day represented in yyyy-MM-dd to integer
 * which is the number of days past the reference day
 */
public class DayIntConverter implements NumericConverter {

    /**
     * Convert date string value to the number of days
     * past the reference day.
     *
     * @param v date string value
     * @return number of days past the reference day
     */
    public int toInt(String v) {
        DateTime end = DateBase.FORMATTER.parseDateTime(v);
        return Days.daysBetween(DateBase.BASE, end).getDays();
    }

    /**
     * Get date according to number of days past the
     * reference day.
     *
     * @param days number of days past the reference day
     * @return date string value for specific format
     */
    public String getString(int days) {
        DateTime dt = DateBase.BASE.plusDays(days);
        return DateBase.FORMATTER.print(dt);
    }
}
