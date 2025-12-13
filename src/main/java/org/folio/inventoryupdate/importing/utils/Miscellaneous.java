package org.folio.inventoryupdate.importing.utils;

import static org.folio.inventoryupdate.importing.service.ImportService.logger;

import java.time.Period;

public final class Miscellaneous {

  private Miscellaneous() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Takes a period in the form of "3 WEEKS", for example, and turns it into a temporal amount.
   *
   * @param periodAsText a string with an integer followed by DAYS, WEEKS, or MONTHS
   * @return temporal amount representing the period.
   */
  public static Period getPeriod(String periodAsText, int defaultAmount, String defaultUnit) {
    if (periodAsText != null) {
      String[] periodAsArray = periodAsText.trim().toUpperCase().split(" ");
      if (periodAsArray.length == 2) {
        try {
          int amount = Integer.parseInt(periodAsArray[0]);
          String unit = periodAsArray[1];
          return switch (unit) {
            case "DAY", "DAYS" -> Period.ofDays(amount);
            case "WEEK", "WEEKS" -> Period.ofWeeks(amount);
            case "MONTH", "MONTHS" -> Period.ofMonths(amount);
            default -> null;
          };
        } catch (NumberFormatException nfe) {
          logger.error("Could not resolve period from [{}]. Expected string on the format: "
              + "<number> DAY(S)|WEEK(S)|MONTH(S)", periodAsText);
        }
      }
    }
    return switch (defaultUnit) {
      case "DAYS" -> Period.ofDays(defaultAmount);
      case "WEEKS" -> Period.ofWeeks(defaultAmount);
      case "MONTHS" -> Period.ofMonths(defaultAmount);
      default -> null;
    };
  }
}
