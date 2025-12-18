package org.folio.inventoryupdate.importing.service.delivery.respond;

import static org.folio.okapi.common.HttpResponse.responseError;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.templates.SqlTemplate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.foliodata.SettingsClient;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.moduledata.LogLine;
import org.folio.inventoryupdate.importing.moduledata.RecordFailure;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.inventoryupdate.importing.utils.Miscellaneous;
import org.folio.inventoryupdate.importing.utils.SettableClock;
import org.folio.tlib.postgres.TenantPgPool;

public class LogPurging  {

  protected static final Logger logger = LogManager.getLogger(EntityStorage.class);
  protected final TenantPgPool pool;
  protected final String tenant;

  public LogPurging(Vertx vertx, String tenant) {
    this.pool = TenantPgPool.pool(vertx, tenant);
    this.tenant = tenant;
  }

  public static Future<Void> purgeAgedLogs(ServiceRequest request) {
    logger.info("Running timer process: purge aged logs");
    final String settings_scope = "mod-inventory-import";
    final String settings_key = "PURGE_LOGS_AFTER";
    return SettingsClient.getStringValue(request.routingContext(), settings_scope, settings_key)
        .compose(purgeSetting ->
            new LogPurging(request.vertx(), request.tenant()).purgePastJobsAndRespond(request, purgeSetting));
  }

  private Future<Void> purgePastJobsAndRespond(ServiceRequest request, String purgeSetting) {
    Period ageForDeletion = Miscellaneous.getPeriod(purgeSetting, 3, "MONTHS");
    LocalDateTime untilDate = SettableClock.getLocalDateTime().minus(ageForDeletion).truncatedTo(ChronoUnit.MINUTES);
    logger.info("Purging aged logs from before {}", untilDate);
    return new LogPurging(request.vertx(), request.tenant()).purgePreviousJobsByAge(untilDate)
        .onComplete(result -> request.routingContext().response().setStatusCode(204).end())
        .onFailure(error -> responseError(request.routingContext(), 500, error.getMessage()));
  }

  private Future<Void> purgePreviousJobsByAge(LocalDateTime untilDate) {
    return deleteLogs(untilDate)
        .compose(this::logDeletions)
        .compose(deletedLogs -> deleteFailedRecords(untilDate))
        .compose(this::logDeletions)
        .compose(deletedFailedRecords -> deleteImportJobs(untilDate))
        .compose(this::logDeletions);
  }

  private Future<Void> logDeletions(String text) {
    logger.info(text);
    return Future.succeededFuture();
  }

  private Future<String> deleteLogs(LocalDateTime untilDate) {
    return SqlTemplate.forUpdate(pool.getPool(),
            "DELETE FROM " + pool.getSchema() + "." + Tables.LOG_STATEMENT
                + " WHERE " + new LogLine().field(LogLine.IMPORT_JOB_ID).columnName()
                + "    IN (SELECT " + new ImportJob().field(ImportJob.ID).columnName()
                + "        FROM " + pool.getSchema() + "." + Tables.IMPORT_JOB
                + "        WHERE " + new ImportJob().field(ImportJob.STARTED).columnName() + " < #{untilDate} )")
        .execute(Collections.singletonMap("untilDate", untilDate))
        .map(result -> result.rowCount() + " log lines deleted.")
        .onFailure(error -> logger.error(error.getMessage() + " (occurred when attempting to delete logs)"));
  }

  private Future<String> deleteFailedRecords(LocalDateTime untilDate) {
    return SqlTemplate.forUpdate(pool.getPool(),
            "DELETE FROM " + pool.getSchema() + "." + Tables.RECORD_FAILURE
                + " WHERE " + new RecordFailure().field(LogLine.IMPORT_JOB_ID).columnName()
                + "    IN (SELECT " + new ImportJob().field(ImportJob.ID).columnName()
                + "        FROM " + pool.getSchema() + "." + Tables.IMPORT_JOB
                + "       WHERE " + new ImportJob().field(ImportJob.STARTED).columnName() + " < #{untilDate} )")
        .execute(Collections.singletonMap("untilDate", untilDate))
        .map(result -> result.rowCount() + " failed records deleted.")
        .onFailure(error -> logger.error(error.getMessage() + " (occurred when attempting to delete failed records)"));
  }

  private Future<String> deleteImportJobs(LocalDateTime untilDate) {
    return SqlTemplate.forUpdate(pool.getPool(),
            "DELETE FROM " + pool.getSchema() + "." + Tables.IMPORT_JOB
                + " WHERE " + new ImportJob().field(ImportJob.STARTED).columnName() + " <#{untilDate} ")
        .execute(Collections.singletonMap("untilDate", untilDate))
        .map(result -> result.rowCount() + " import jobs deleted.")
        .onFailure(error -> logger.error(error.getMessage() + " (occurred when attempting to delete import jobs)"));
  }
}
