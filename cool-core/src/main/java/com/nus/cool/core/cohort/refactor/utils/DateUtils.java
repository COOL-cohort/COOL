package com.nus.cool.core.cohort.refactor.utils;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;


public class DateUtils {
    /**
     * Create a LocalDateTime Instance from a unix timestamp
     * 
     * @param unixTime
     * @return
     */
    // public static Local
    public static LocalDateTime createCalender(long unixTime) {
        // LocalDateTime.ofInstant(, zone)
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTime),
                TimeZone.getDefault().toZoneId());
    }

    public static LocalDateTime createCalender(int days){
        long unixTime = days * 24 * 3600;
        return createCalender(unixTime);
    }

    public static int getPart(LocalDateTime time, TimeUtils.TimeUnit unit) {
        switch (unit) {
            case DAY:
                return time.getDayOfMonth();
            case HOUR:
                return time.getHour();
            case MINUTE:
                return time.getMinute();
            case MONTH:
                return time.getMonthValue();
            case SECOND:
                return time.getSecond();
            case WEEK:
                return (int) (time.getDayOfYear() / 7) + 1;
            case YEAR:
                return time.getYear();
            default:
                throw new IllegalStateException("Illegal TimeUnit");
        }
    }

    /**
     * 
     * @param ts1
     *            the start localDateTime
     * @param ts2
     *            the compared lis duration.
     * @param unit
     *            duration's unit
     * @return a TimeWindow to descripte duration
     */
    public static TimeWindow getDifference(LocalDateTime ts1, LocalDateTime ts2, TimeUtils.TimeUnit unit) {
        Duration d = Duration.between(ts1, ts2);
        switch (unit) {
            case DAY:
                return new TimeWindow(d.toDays(), TimeUtils.TimeUnit.DAY);
            case HOUR:
                return new TimeWindow(d.toHours(), TimeUtils.TimeUnit.HOUR);
            case MINUTE:
                return new TimeWindow(d.toMinutes(), TimeUtils.TimeUnit.MINUTE);
            case MONTH:
                return new TimeWindow(d.toDays() / 30, TimeUtils.TimeUnit.MONTH);
            case SECOND:
                return new TimeWindow(d.toSeconds(), TimeUtils.TimeUnit.SECOND);
            case WEEK:
                return new TimeWindow(d.toDays() / 7, TimeUtils.TimeUnit.WEEK);
            case YEAR:
                return new TimeWindow(d.toDays() / 365, TimeUtils.TimeUnit.YEAR);
            default:
                throw new IllegalStateException("Illegal TimeUnit");

        }
    }

    public static String convertString(LocalDateTime date) {
        return date.toString();
    }

}
