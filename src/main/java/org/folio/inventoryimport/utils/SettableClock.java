package org.folio.inventoryimport.utils;

import java.time.*;
import java.time.temporal.ChronoUnit;

public class SettableClock {
    private static java.time.Clock clock = java.time.Clock.systemUTC();

    /**
     * Set the clock assigned to the clock manager to a given clock.
     */
    public static void setClock(Clock clock) {
        if (clock == null) {
            throw new IllegalArgumentException("clock cannot be null");
        }

        SettableClock.clock = clock;
    }

    /**
     * Set the clock assigned to the clock manager to the system clock.
     */
    public static void setDefaultClock() {
        clock = Clock.systemUTC();
    }

    /**
     * Get the clock assigned the clock manager.
     *
     * @return The clock currently being used by ClockManager.
     */
    public static Clock getClock() {
        return clock;
    }

    /**
     * Get the current system time according to the clock manager.
     *
     * @return A ZonedDateTime as if now() is called.
     * Time is truncated to milliseconds.
     */
    public static ZonedDateTime getZonedDateTime() {
        return ZonedDateTime.now(clock).truncatedTo(ChronoUnit.MILLIS);
    }

    /**
     * Get the current system time according to the clock manager.
     *
     * @return A LocalDateTime as if now() is called.
     * Time is truncated to milliseconds.
     */
    public static LocalDateTime getLocalDateTime() {
        return LocalDateTime.now(clock).truncatedTo(ChronoUnit.MILLIS);
    }

    /**
     * Get the current system time according to the clock manager.
     *
     * @return A LocalDate as if now() is called.
     */
    public static LocalDate getLocalDate() {
        return LocalDate.now(clock);
    }

    /**
     * Get the current system time according to the clock manager.
     *
     * @return A LocalTime as if now() is called.
     * Time is truncated to milliseconds.
     */
    public static LocalTime getLocalTime() {
        return LocalTime.now(clock).truncatedTo(ChronoUnit.MILLIS);
    }

    /**
     * Get the current system time according to the clock manager.
     *
     * @return An Instant as if now() is called.
     * Time is truncated to milliseconds.
     */
    public static Instant getInstant() {
        return clock.instant();
    }

    /**
     * Get the time zone of the system clock according to the clock manager.
     *
     * @return The current time zone as a ZoneId.
     */
    public static ZoneId getZoneId() {
        return clock.getZone();
    }

    /**
     * Get the time zone of the system clock according to the clock manager.
     *
     * @return The current time zone as a ZoneOffset.
     */
    public static ZoneOffset getZoneOffset() {
        return ZoneOffset.of(clock.getZone().getRules().getOffset(clock.instant())
                .getId());
    }


}
