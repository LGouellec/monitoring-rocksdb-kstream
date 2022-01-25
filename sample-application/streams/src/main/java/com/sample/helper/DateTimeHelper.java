package com.sample.helper;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateTimeHelper {

    /**
     * Get the epoch milliseconds from UTC day 00:00
     * @param date day with format dd-MM-yyyy
     * @return epoch milliseconds in UTC timezone
     */
    public static long getEpochMidnight(String date){
        LocalDate ldt = LocalDate.parse(date, DateTimeFormatter.ofPattern("dd-MM-yyyy"));
        ZonedDateTime utcTime = ldt.atTime(LocalTime.MIDNIGHT).atZone(ZoneId.of("UTC"));
        return utcTime.toInstant().toEpochMilli();
    }
}
