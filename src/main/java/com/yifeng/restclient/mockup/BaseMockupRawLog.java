package com.yifeng.restclient.mockup;

import java.time.*;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by guoyifeng on 1/5/19
 */
public class BaseMockupRawLog {
    public static Date atStartOfDay(Date date) {
        LocalDateTime localDateTime = dateToLocalDateTime(date);
        LocalDateTime startOfDay = localDateTime.with(LocalTime.MIN);
        return localDateTimeToDate(startOfDay);
    }

    public static Date atEndOfDay(Date date) {
        LocalDateTime localDateTime = dateToLocalDateTime(date);
        LocalDateTime endOfDay = localDateTime.with(LocalTime.MAX);
        return localDateTimeToDate(endOfDay);
    }

    public static Date asDate(LocalDate localDate) {
        return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
    }

    public static LocalDateTime dateToLocalDateTime(Date date) {
        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }

    public static Date localDateTimeToDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static long randomToday() {
        Date now = new Date();
        return ThreadLocalRandom.current().nextLong(atStartOfDay(now).getTime(), atEndOfDay(now).getTime());
    }

    public static long randamThatDay(Date date) {
        return ThreadLocalRandom.current().nextLong(atStartOfDay(date).getTime(), atEndOfDay(date).getTime());
    }
}
