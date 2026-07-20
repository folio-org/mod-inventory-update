package org.folio.inventoryupdate.importing.service.delivery.respond;

import io.vertx.core.Future;
import io.vertx.sqlclient.templates.SqlTemplate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.foliodata.SettingsClient;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.inventoryupdate.importing.utils.Miscellaneous;
import org.folio.inventoryupdate.importing.utils.SettableClock;

public class LogPurging  {

  protected static final Logger logger = LogManager.getLogger(LogPurging.class);

  public static Future<Void> purgeAgedLogs(ServiceRequest request) {
    logger.info("Running process: purge aged logs");
    final String settings_scope = "mod-inventory-update";
    final String settings_key = "PURGE_LOGS_AFTER";
    return SettingsClient.getStringValue(request.routingContext(), settings_scope, settings_key)
        .map(LogPurging::getCutOffDate)
        .compose(cutOff -> purgePreviousJobsByAge(request, cutOff).map(cutOff))
        .onSuccess(result -> request.routingContext().response().setStatusCode(204).end())
        .mapEmpty();
  }

  private static LocalDateTime getCutOffDate(String purgeSetting) {
    Period ageForDeletion = Miscellaneous.getPeriod(purgeSetting, 3, "MONTHS");
    return SettableClock.getLocalDateTime().minus(ageForDeletion).truncatedTo(ChronoUnit.MINUTES);
  }

  private static Future<Void> purgePreviousJobsByAge(ServiceRequest request, LocalDateTime untilDate) {
    return SqlTemplate.forUpdate(request.entityStorage().getTenantPool().getPool(),
            "DELETE FROM " + request.entityStorage().getTenantPool().getSchema() + "." + Tables.IMPORT_JOB
                + " WHERE " + new ImportJob().field(ImportJob.STARTED).columnName() + " <#{untilDate} ")
        .execute(Collections.singletonMap("untilDate", untilDate))
        .onSuccess(result -> logger.info("{} import jobs deleted", result.rowCount()))
        .onFailure(error -> logger.error("{} (occurred when attempting to delete import jobs)", error.getMessage()))
        .mapEmpty();
  }
}
