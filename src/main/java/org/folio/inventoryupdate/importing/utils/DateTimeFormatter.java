package org.folio.inventoryupdate.importing.utils;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatterBuilder;

public final class DateTimeFormatter {

  public static final java.time.format.DateTimeFormatter TIME_MINUTES;

  static {
    TIME_MINUTES = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendValue(HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2)
        .toFormatter();
  }

  public static final java.time.format.DateTimeFormatter TIME_SECONDS;

  static {
    TIME_SECONDS = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(TIME_MINUTES)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2)
        .toFormatter();
  }

  public static final java.time.format.DateTimeFormatter TIME;

  static {

    TIME = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(TIME_SECONDS)
        .optionalStart()
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .parseLenient()
        .appendOffset("+HH:MM", "+00:00")
        .parseStrict()
        .toFormatter();
  }

  public static final java.time.format.DateTimeFormatter DATE_TIME;

  static {
    DATE_TIME = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .appendLiteral('T')
        .append(TIME)
        .toFormatter();
  }

  private DateTimeFormatter() {
    throw new IllegalStateException("Utility class");
  }

  public static String formatDateTime(ZonedDateTime dateTime) {
    return dateTime.format(DATE_TIME);
  }

  /**
   * Takes a LocalDataTime and assumes it's in UTC and converts it to a zoned date time.
   *
   * @param dateTime a local date-time assumed to be UTC.
   * @return formatted zoned date-time
   */
  public static String formatDateTime(LocalDateTime dateTime) {
    return formatDateTime(dateTime.atOffset(ZoneOffset.UTC).toZonedDateTime());
  }
}
