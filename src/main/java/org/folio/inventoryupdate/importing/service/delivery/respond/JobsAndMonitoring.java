package org.folio.inventoryupdate.importing.service.delivery.respond;

import static org.folio.inventoryupdate.importing.service.delivery.respond.Channels.getChannelByTagOrUuid;
import static org.folio.okapi.common.HttpResponse.responseJson;
import static org.folio.okapi.common.HttpResponse.responseText;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.moduledata.LogLine;
import org.folio.inventoryupdate.importing.moduledata.RecordFailure;
import org.folio.inventoryupdate.importing.moduledata.database.Entity;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.moduledata.database.SqlQuery;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileListeners;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileProcessor;
import org.folio.tlib.postgres.PgCqlException;

public final class JobsAndMonitoring extends EntityResponses {

  public static final Logger logger = LogManager.getLogger("jobs-and-logging");

  private JobsAndMonitoring() {
    throw new UnsupportedOperationException("Static storage utilities");
  }

  public static Future<Void> postImportJob(ServiceRequest request) {
    ImportJob importJob = new ImportJob().fromJson(request.bodyAsJson());
    return storeEntityRespondWith201(request, importJob);
  }

  public static Future<Void> getImportJobs(ServiceRequest request) {
    EntityStorage db = request.entityStorage();

    String fromDateTime = request.queryParam("from");
    String untilDateTime = request.queryParam("until");
    String timeRange = null;
    if (fromDateTime != null && untilDateTime != null) {
      timeRange = " (finished >= '" + fromDateTime + "'  AND finished <= '" + untilDateTime + "') ";
    } else if (fromDateTime != null) {
      timeRange = " finished >= '" + fromDateTime + "' ";
    } else if (untilDateTime != null) {
      timeRange = " finished <= '" + untilDateTime + "' ";
    }

    SqlQuery query;
    try {
      query = new ImportJob()
          .cqlToSql(request, db.schemaDotTable(Tables.IMPORT_JOB)).withAdditionalWhereClause(timeRange);
    } catch (PgCqlException pce) {
      responseText(request.routingContext(), 400)
          .end("Could not execute query to retrieve jobs: " + pce.getMessage() + " Request:" + request.absoluteUri());
      return Future.succeededFuture();
    } catch (Exception e) {
      return Future.failedFuture(e.getMessage());
    }
    return db.getEntities(query.getQueryWithLimits(), new ImportJob()).onComplete(jobsList -> {
      if (jobsList.succeeded()) {
        JsonObject responseJson = new JsonObject();
        JsonArray importJobs = new JsonArray();
        responseJson.put("importJobs", importJobs);
        List<Entity> jobs = jobsList.result();
        for (Entity job : jobs) {
          importJobs.add(job.asJson());
        }
        db.getCount(query.getCountingSql()).onComplete(count -> {
          responseJson.put("totalRecords", count.result());
          responseJson(request.routingContext(), 200).end(responseJson.encodePrettily());
        });
      } else {
        responseText(request.routingContext(), 500).end("Problem retrieving jobs: " + jobsList.cause().getMessage());
      }
    }).mapEmpty();
  }

  public static Future<Void> getImportJobById(ServiceRequest request) {
    return getEntityAndRespond(request, new ImportJob());
  }

  public static Future<Void> deleteImportJob(ServiceRequest request) {
    return deleteEntityAndRespond(request, new ImportJob());
  }

  public static Future<Void> postLogStatements(ServiceRequest request) {
    JsonObject body = request.bodyAsJson();
    JsonArray lines = body.getJsonArray("logLines");
    List<Entity> logLines = new ArrayList<>();
    for (Object o : lines) {
      logLines.add(new LogLine().fromJson((JsonObject) o).withCreatingUser(request.currentUser()));
    }
    return request.entityStorage().storeEntities(logLines)
        .onSuccess(configId ->
            responseJson(request.routingContext(), 201).end(logLines.size() + " log line(s) created."))
        .mapEmpty();
  }

  public static Future<Void> getLogStatements(ServiceRequest request) {
    EntityStorage db = request.entityStorage();
    SqlQuery queryFromCql =
        new LogLine().cqlToSql(request, db.schemaDotTable(Tables.JOB_LOG_VIEW)).withDefaultLimit("100");
    String from = request.queryParam("from");
    String until = request.queryParam("until");

    String timeRange = null;
    if (from != null && until != null) {
      timeRange = " (time_stamp >= '" + from + "'  AND time_stamp <= '" + until + "') ";
    } else if (from != null) {
      timeRange = " time_stamp >= '" + from + "' ";
    } else if (until != null) {
      timeRange = " time_stamp <= '" + until + "' ";
    }

    if (timeRange != null) {
      queryFromCql.withAdditionalWhereClause(timeRange);
    }

    return db.getEntities(queryFromCql.getQueryWithLimits(), new LogLine()).onComplete(logStatements -> {
      boolean asText = request.getHeader("Accept").equalsIgnoreCase("text/plain");
      if (logStatements.succeeded()) {
        JsonObject responseJson = new JsonObject();
        final StringBuilder logAsText = new StringBuilder();
        JsonArray logLines = new JsonArray();
        responseJson.put("logLines", logLines);
        for (Entity logLine : logStatements.result()) {
          if (asText) {
            logAsText.append(logLine.toString()).append(System.lineSeparator());
          } else {
            logLines.add(logLine.asJson());
          }
        }
        if (asText) {
          responseText(request.routingContext(), 200).end(logAsText.toString());
        } else {
          db.getCount(queryFromCql.getCountingSql()).onComplete(count -> {
            responseJson.put("totalRecords", count.result());
            responseJson(request.routingContext(), 200).end(responseJson.encodePrettily());
          });
        }
      }
    }).mapEmpty();
  }

  public static Future<Void> getFailedRecords(ServiceRequest request) {
    EntityStorage db = request.entityStorage();
    SqlQuery queryFromCql = new RecordFailure()
        .cqlToSql(request, db.schemaDotTable(Tables.RECORD_FAILURE_VIEW)).withDefaultLimit("100");
    String jobId = request.requestParam("id");
    String from = request.queryParam("from");
    String until = request.queryParam("until");

    String timeRange = null;
    if (from != null && until != null) {
      timeRange = " (time_stamp >= '" + from + "'  AND time_stamp <= '" + until + "') ";
    } else if (from != null) {
      timeRange = " time_stamp >= '" + from + "' ";
    } else if (until != null) {
      timeRange = " time_stamp <= '" + until + "' ";
    }

    if (jobId != null) {
      queryFromCql.withAdditionalWhereClause("import_job_id = '" + jobId + "'");
    }
    if (timeRange != null) {
      queryFromCql.withAdditionalWhereClause(timeRange);
    }

    return db.getEntities(queryFromCql.getQueryWithLimits(), new RecordFailure()).onComplete(failuresList -> {
      if (failuresList.succeeded()) {
        JsonObject responseJson = new JsonObject();
        JsonArray recordFailures = new JsonArray();
        responseJson.put("failedRecords", recordFailures);
        List<Entity> failures = failuresList.result();
        for (Entity failure : failures) {
          recordFailures.add(failure.asJson());
        }
        db.getCount(queryFromCql.getCountingSql()).onComplete(count -> {
          responseJson.put("totalRecords", count.result());
          responseJson(request.routingContext(), 200).end(responseJson.encodePrettily());
        });
      }
    }).mapEmpty();
  }

  public static Future<Void> postFailedRecords(ServiceRequest request) {
    JsonArray recs = request.bodyAsJson().getJsonArray(new RecordFailure().jsonCollectionName());
    List<Entity> failedRecs = new ArrayList<>();
    for (Object o : recs) {
      failedRecs.add(new RecordFailure().fromJson((JsonObject) o).withCreatingUser(request.currentUser()));
    }
    return request.entityStorage().storeEntities(failedRecs)
        .onSuccess(configId ->
            responseJson(request.routingContext(), 201).end(failedRecs.size() + " record failures logged."))
        .mapEmpty();
  }

  public static Future<Void> deleteRecordFailure(ServiceRequest request) {
    return deleteEntityAndRespond(request, new RecordFailure());
  }

  public static Future<Void> pauseImportJob(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext(), 404)
            .end("Found no channel with ID " + channelId + " to pause job for.").mapEmpty();
      } else {
        UUID channelUuid = channel.getId();
        if (FileListeners.hasFileListener(request.tenant(), channelUuid.toString())) {
          FileProcessor processor = FileListeners
              .getFileListener(request.tenant(), channelUuid.toString()).getProcessor();
          if (processor == null || !processor.getImportJob().markedRunning()) {
            return responseText(request.routingContext(), 404)
                .end("No running job to pause found for this channel, [" + channelUuid + "].");
          } else {
            processor.pause();
            return responseText(request.routingContext(), 200)
                .end("Processing paused for channel [" + channelUuid + "].");
          }
        } else {
          return responseText(request.routingContext(), 404)
              .end("Channel is not commissioned [" + channelUuid + "].");
        }
      }
    });
  }

  public static Future<Void> resumeImportJob(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext(), 404)
            .end("Found no channel with ID " + channelId + " to resume job for.").mapEmpty();
      } else {
        UUID channelUuid = channel.getId();
        if (FileListeners.hasFileListener(request.tenant(), channelUuid.toString())) {
          FileProcessor processor = FileListeners
              .getFileListener(request.tenant(), channelUuid.toString()).getProcessor();
          if (processor != null && processor.paused()) {
            processor.resume();
            return responseText(request.routingContext(), 200)
                .end("Processing resumed for channel [" + channelId + "].");
          } else {
            return responseText(request.routingContext(), 404)
                .end("No paused job to resume found for this channel [" + channelId + "].");
          }
        } else {
          return responseText(request.routingContext(), 404)
              .end("Channel is not commissioned [" + channelUuid + "].");
        }
      }
    });
  }
}
