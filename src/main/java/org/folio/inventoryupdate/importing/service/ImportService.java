package org.folio.inventoryupdate.importing.service;

import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;
import static org.folio.okapi.common.HttpResponse.responseText;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.impl.BodyHandlerImpl;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.contract.OpenAPIContract;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.foliodata.ConfigurationsClient;
import org.folio.inventoryupdate.importing.foliodata.SettingsClient;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.moduledata.Entity;
import org.folio.inventoryupdate.importing.moduledata.ImportJob;
import org.folio.inventoryupdate.importing.moduledata.LogLine;
import org.folio.inventoryupdate.importing.moduledata.RecordFailure;
import org.folio.inventoryupdate.importing.moduledata.Step;
import org.folio.inventoryupdate.importing.moduledata.Transformation;
import org.folio.inventoryupdate.importing.moduledata.TransformationStep;
import org.folio.inventoryupdate.importing.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryupdate.importing.moduledata.database.SqlQuery;
import org.folio.inventoryupdate.importing.moduledata.database.Tables;
import org.folio.inventoryupdate.importing.service.fileimport.FileListener;
import org.folio.inventoryupdate.importing.service.fileimport.FileListeners;
import org.folio.inventoryupdate.importing.service.fileimport.FileProcessor;
import org.folio.inventoryupdate.importing.service.fileimport.FileQueue;
import org.folio.inventoryupdate.importing.utils.Miscellaneous;
import org.folio.inventoryupdate.importing.utils.SettableClock;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;
import org.folio.tlib.postgres.PgCqlException;

/**
 * Main service.
 */
public class ImportService implements RouterCreator, TenantInitHooks {

  public static final Logger logger = LogManager.getLogger("inventory-import");

  @Override
  public Future<Router> createRouter(Vertx vertx) {
    return OpenAPIContract.from(vertx, "openapi/inventory-import-1.0.yaml").map(contract -> {
      RouterBuilder routerBuilder = RouterBuilder.create(vertx, contract);
      handlers(vertx, routerBuilder);
      return routerBuilder.createRouter();
    }).onSuccess(res -> logger.info("OpenAPI parsed OK"));
  }

  private void handlers(Vertx vertx, RouterBuilder routerBuilder) {
    // Configurations
    validatingHandler(vertx, routerBuilder, "postChannel", this::postChannel);
    validatingHandler(vertx, routerBuilder, "getChannels", this::getChannels);
    validatingHandler(vertx, routerBuilder, "getChannel", this::getChannelById);
    validatingHandler(vertx, routerBuilder, "putChannel", this::putChannel);
    validatingHandler(vertx, routerBuilder, "deleteChannel", this::deleteChannel);
    validatingHandler(vertx, routerBuilder, "postTransformation", this::postTransformation);
    validatingHandler(vertx, routerBuilder, "getTransformation", this::getTransformationById);
    validatingHandler(vertx, routerBuilder, "getTransformations", this::getTransformations);
    validatingHandler(vertx, routerBuilder, "putTransformation", this::updateTransformation);
    validatingHandler(vertx, routerBuilder, "deleteTransformation", this::deleteTransformation);
    validatingHandler(vertx, routerBuilder, "postStep", this::postStep);
    validatingHandler(vertx, routerBuilder, "getSteps", this::getSteps);
    validatingHandler(vertx, routerBuilder, "getStep", this::getStepById);
    validatingHandler(vertx, routerBuilder, "putStep", this::putStep);
    validatingHandler(vertx, routerBuilder, "deleteStep", this::deleteStep);
    validatingHandler(vertx, routerBuilder, "getScript", this::getScript);
    nonValidatingHandler(vertx, routerBuilder, "putScript", this::putScript);
    validatingHandler(vertx, routerBuilder, "postTsa", this::postTransformationStep);
    validatingHandler(vertx, routerBuilder, "getTsas", this::getTransformationSteps);
    validatingHandler(vertx, routerBuilder, "getTsa", this::getTransformationStepById);
    validatingHandler(vertx, routerBuilder, "putTsa", this::putTransformationStep);
    validatingHandler(vertx, routerBuilder, "deleteTsa", this::deleteTransformationStep);

    // Jobs
    validatingHandler(vertx, routerBuilder, "getImportJobs", this::getImportJobs);
    validatingHandler(vertx, routerBuilder, "getImportJob", this::getImportJobById);
    validatingHandler(vertx, routerBuilder, "postImportJob", this::postImportJob);
    validatingHandler(vertx, routerBuilder, "deleteImportJob", this::deleteImportJob);
    nonValidatingHandler(vertx, routerBuilder, "postImportJobLogLines", this::postLogStatements);
    nonValidatingHandler(vertx, routerBuilder, "getImportJobLogLines", this::getLogStatements);
    validatingHandler(vertx, routerBuilder, "getFailedRecords", this::getFailedRecords);
    validatingHandler(vertx, routerBuilder, "postFailedRecords", this::postFailedRecords);
    validatingHandler(vertx, routerBuilder, "deleteRecordFailure", this::deleteRecordFailure);
    // Processing
    validatingHandler(vertx, routerBuilder, "purgeAgedLogs", this::purgeAgedLogs);
    nonValidatingHandler(vertx, routerBuilder, "uploadXmlRecords", this::uploadXmlSourceFile);
    validatingHandler(vertx, routerBuilder, "deployFileListener", this::deployFileListener);
    validatingHandler(vertx, routerBuilder, "undeployFileListener", this::undeployFileListener);
    validatingHandler(vertx, routerBuilder, "pauseJob", this::pauseImportJob);
    validatingHandler(vertx, routerBuilder, "resumeJob", this::resumeImportJob);
    validatingHandler(vertx, routerBuilder, "initFileSystemQueue", this::initFileSystemQueue);
  }

  private void validatingHandler(Vertx vertx, RouterBuilder routerBuilder, String operation,
                                 Function<ServiceRequest, Future<Void>> method) {
    routerBuilder.getRoute(operation).addHandler(ctx -> {
      try {
        method.apply(new RequestValidated(vertx, ctx)).onFailure(cause -> {
          logger.error("Handler failure {}: {}", operation, cause.getMessage());
          exceptionResponse(cause, ctx);
        });
      } catch (Exception e) {
        logger.error("Handler exception {}: {}", operation, e.getMessage(), e);
        exceptionResponse(e, ctx);
      }
    }).addFailureHandler(this::routerExceptionResponse);
  }

  /**
   * For POSTing text, PUTting xml, decoding the CQL query parameter.
   */
  private void nonValidatingHandler(Vertx vertx, RouterBuilder routerBuilder, String operation,
                                    Function<ServiceRequest, Future<Void>> method) {
    routerBuilder.getRoute(operation).addHandler(new BodyHandlerImpl().setBodyLimit(104857600)).setDoValidation(false)
        .addHandler(ctx -> {
          try {
            method.apply(new RequestUnvalidated(vertx, ctx)).onFailure(cause -> {
              logger.error("Non-validating handler failure {}: {}", operation, cause.getMessage());
              exceptionResponse(cause, ctx);
            });
          } catch (Exception e) {  // exception thrown by method
            logger.error("Non-validating handler exception {}: {}", operation, e.getMessage(), e);
            exceptionResponse(e, ctx);
          }
        }).addFailureHandler(this::routerExceptionResponse);
  }

  private void exceptionResponse(Throwable cause, RoutingContext routingContext) {
    if (cause.getMessage().toLowerCase().contains("could not find")) {
      responseError(routingContext, 404, cause.getMessage());
    } else {
      responseError(routingContext, 400, cause.getClass().getSimpleName() + ": " + cause.getMessage());
    }
  }

  /**
   * OAS validation exception.
   */
  private void routerExceptionResponse(RoutingContext ctx) {
    if (ctx.failure() != null) {
      String message = ctx.failure().getMessage();
      responseError(ctx, ctx.statusCode(), message + ": "
          + (ctx.failure().getCause() != null ? ctx.failure().getCause().getMessage() : " (no cause provided)"));
    } else {
      responseError(ctx, ctx.statusCode(), " router exception");
    }
  }

  @Override
  public Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    return new ModuleStorageAccess(vertx, tenant).init(tenantAttributes).onFailure(x ->
            logger.error("Database initialization failed: {}", x.getMessage())).onSuccess(x ->
            logger.info("Tenant '{}' database initialized", tenant))
        .compose(x ->
            clearTenantFileQueues(vertx, tenant, getTenantParameter(tenantAttributes, "clearPastFileQueues")));
  }

  private static String getTenantParameter(JsonObject attributes, String parameterKey) {
    if (attributes.containsKey("parameters") && attributes.getValue("parameters") instanceof JsonArray) {
      JsonArray parameters = attributes.getJsonArray("parameters");
      for (int i = 0; i < parameters.size(); i++) {
        JsonObject parameter = parameters.getJsonObject(i);
        if (parameterKey.equals(parameter.getString("key"))) {
          return parameter.getString("value");
        }
      }
    }
    return null;
  }

  public Future<Void> clearTenantFileQueues(Vertx vertx, String tenant, String clearPastFileQueues) {
    if ("true".equalsIgnoreCase(clearPastFileQueues)) {
      FileQueue.clearTenantQueues(vertx, tenant);
    }
    return Future.succeededFuture();
  }

  private Future<Void> getEntities(ServiceRequest request, Entity entity) {
    ModuleStorageAccess db = request.moduleStorageAccess();
    SqlQuery query;
    try {
      query = entity.cqlToSql(request, db.schemaDotTable(entity.table()));
    } catch (PgCqlException pce) {
      responseText(request.routingContext, 400)
          .end("Could not execute query to retrieve " + entity.jsonCollectionName() + ": " + pce.getMessage()
              + " Request:" + request.absoluteUri());
      return Future.succeededFuture();
    } catch (Exception e) {
      return Future.failedFuture(e.getMessage());
    }
    return db.getEntities(query.getQueryWithLimits(), entity).onComplete(result -> {
      if (result.succeeded()) {
        JsonObject responseJson = new JsonObject();
        JsonArray jsonRecords = new JsonArray();
        responseJson.put(entity.jsonCollectionName(), jsonRecords);
        List<Entity> recs = result.result();
        for (Entity rec : recs) {
          jsonRecords.add(rec.asJson());
        }
        db.getCount(query.getCountingSql()).onComplete(count -> {
          responseJson.put("totalRecords", count.result());
          responseJson(request.routingContext, 200).end(responseJson.encodePrettily());
        });
      } else {
        responseText(request.routingContext, 500).end("Problem retrieving jobs: " + result.cause().getMessage());
      }
    }).mapEmpty();
  }

  private Future<Void> getEntity(ServiceRequest request, Entity entity) {
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.moduleStorageAccess().getEntity(id, entity).onSuccess(instance -> {
      if (instance == null) {
        responseText(request.routingContext, 404).end(entity.entityName() + " " + id + " not found.");
      } else {
        responseJson(request.routingContext, 200).end(instance.asJson().encodePrettily());
      }
    }).mapEmpty();
  }

  private Future<Void> deleteEntity(ServiceRequest request, Entity entity) {
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.moduleStorageAccess().deleteEntity(id, entity).onSuccess(result -> {
      if (result == 0) {
        responseText(request.routingContext, 404).end("Not found");
      } else {
        responseText(request.routingContext, 200).end();
      }
    }).mapEmpty();
  }

  private Future<Void> storeEntityRespondWith201(ServiceRequest request, Entity entity) {
    ModuleStorageAccess db = request.moduleStorageAccess();
    return db.storeEntity(entity.withCreatingUser(request.currentUser()))
        .onSuccess(id -> db.getEntity(id, entity).map(stored ->
            responseJson(request.routingContext, 201).end(stored.asJson().encodePrettily()))).mapEmpty();
  }

  private Future<Void> postChannel(ServiceRequest request) {
    Channel channel = new Channel().fromJson(request.bodyAsJson());
    ModuleStorageAccess db = request.moduleStorageAccess();
    return db.storeEntity(channel.withCreatingUser(request.currentUser())).compose(id -> {
      new FileQueue(request, id.toString()).createDirectoriesIfNotExist();
      return Future.succeededFuture(id);
    }).compose(id -> db.getEntity(id, channel)).compose(cfg -> {
      FileListeners.deployIfNotDeployed(request, (Channel) cfg);
      return Future.succeededFuture(cfg);
    }).compose(cfg -> responseJson(request.routingContext, 201).end(cfg.asJson().encodePrettily())).mapEmpty();
  }

  private Future<Void> getChannels(ServiceRequest request) {
    return getEntities(request, new Channel());
  }

  private Future<Void> getChannelById(ServiceRequest request) {
    return getEntity(request, new Channel());
  }

  private Future<Void> putChannel(ServiceRequest request) {
    Channel channel = new Channel().fromJson(request.bodyAsJson());
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.moduleStorageAccess().updateEntity(id, channel.withUpdatingUser(request.currentUser()))
        .onSuccess(result -> {
          if (result.rowCount() == 1) {
            request.moduleStorageAccess().getEntity(id, new Channel()).compose(cfg -> {
              FileListener listener = FileListeners.getFileListener(request.tenant(), id.toString());
              if (listener != null) {
                listener.updateChannel((Channel) cfg);
              }
              return responseText(request.routingContext(), 204).end();
            });
          } else {
            responseText(request.routingContext(), 404).end("Import config to update not found");
          }
        }).mapEmpty();
  }

  private Future<Void> deleteChannel(ServiceRequest request) {
    return deleteEntity(request, new Channel()).compose(na -> {
      new FileQueue(request, request.requestParam("id")).deleteDirectoriesIfExist();
      return undeployFileListener(request);
    }).mapEmpty();
  }

  private Future<Void> postImportJob(ServiceRequest request) {
    ImportJob importJob = new ImportJob().fromJson(request.bodyAsJson());
    return storeEntityRespondWith201(request, importJob);
  }

  private Future<Void> getImportJobs(ServiceRequest request) {
    ModuleStorageAccess db = request.moduleStorageAccess();

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

  private Future<Void> getImportJobById(ServiceRequest request) {
    return getEntity(request, new ImportJob());
  }

  private Future<Void> deleteImportJob(ServiceRequest request) {
    return deleteEntity(request, new ImportJob());
  }

  private Future<Void> postLogStatements(ServiceRequest request) {
    JsonObject body = request.bodyAsJson();
    JsonArray lines = body.getJsonArray("logLines");
    List<Entity> logLines = new ArrayList<>();
    for (Object o : lines) {
      logLines.add(new LogLine().fromJson((JsonObject) o).withCreatingUser(request.currentUser()));
    }
    return request.moduleStorageAccess().storeEntities(logLines)
        .onSuccess(configId ->
            responseJson(request.routingContext(), 201).end(logLines.size() + " log line(s) created."))
        .mapEmpty();
  }

  private Future<Void> getLogStatements(ServiceRequest request) {
    ModuleStorageAccess db = request.moduleStorageAccess();
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
          responseText(request.routingContext, 200).end(logAsText.toString());
        } else {
          db.getCount(queryFromCql.getCountingSql()).onComplete(count -> {
            responseJson.put("totalRecords", count.result());
            responseJson(request.routingContext(), 200).end(responseJson.encodePrettily());
          });
        }
      }
    }).mapEmpty();
  }

  private Future<Channel> getChannelByTagOrUuid(ServiceRequest request, String channelIdentifier) {
    ModuleStorageAccess db = request.moduleStorageAccess();
    SqlQuery queryFromCql = new Channel().cqlToSql(
        channelIdentifier.length() > 24 ? "id==" + channelIdentifier : "tag==" + channelIdentifier, "0", "1",
        db.schemaDotTable(Tables.CHANNEL), new Channel().getQueryableFields());
    return db.getEntities(queryFromCql.getQueryWithLimits(), new Channel()).map(entities -> {
      if (entities.isEmpty()) {
        return null;
      } else {
        return (Channel) (entities.getFirst());
      }
    });
  }

  private Future<Void> getFailedRecords(ServiceRequest request) {

    ModuleStorageAccess db = request.moduleStorageAccess();
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

  private Future<Void> postFailedRecords(ServiceRequest request) {
    JsonArray recs = request.bodyAsJson().getJsonArray(new RecordFailure().jsonCollectionName());
    List<Entity> failedRecs = new ArrayList<>();
    for (Object o : recs) {
      failedRecs.add(new RecordFailure().fromJson((JsonObject) o).withCreatingUser(request.currentUser()));
    }
    return request.moduleStorageAccess().storeEntities(failedRecs)
        .onSuccess(configId ->
            responseJson(request.routingContext(), 201).end(failedRecs.size() + " record failures logged."))
        .mapEmpty();
  }

  private Future<Void> deleteRecordFailure(ServiceRequest request) {
    return deleteEntity(request, new RecordFailure());
  }

  private Future<Void> purgeAgedLogs(ServiceRequest request) {
    logger.info("Running timer process: purge aged logs");
    final String settings_scope = "mod-inventory-import";
    final String settings_key = "PURGE_LOGS_AFTER";
    SettingsClient.getStringValue(request.routingContext(), settings_scope, settings_key).onComplete(settingsValue -> {
      if (settingsValue.result() != null) {
        applyPurgeOfPastJobs(request, settingsValue.result());
      } else {
        final String configs_module = "mod-inventory-import";
        final String configs_config_name = "PURGE_LOGS_AFTER";
        ConfigurationsClient.getStringValue(request.routingContext(), configs_module, configs_config_name)
            .onComplete(configsValue -> applyPurgeOfPastJobs(request, configsValue.result()));
      }
    });
    return Future.succeededFuture();
  }

  private void applyPurgeOfPastJobs(ServiceRequest request, String purgeSetting) {
    Period ageForDeletion = Miscellaneous.getPeriod(purgeSetting, 3, "MONTHS");
    LocalDateTime untilDate = SettableClock.getLocalDateTime().minus(ageForDeletion).truncatedTo(ChronoUnit.MINUTES);
    logger.info("Running timer process: purging aged logs from before {}", untilDate);
    request.moduleStorageAccess().purgePreviousJobsByAge(untilDate)
        .onComplete(x -> request.routingContext().response().setStatusCode(204).end()).mapEmpty();
  }

  private Future<Void> postStep(ServiceRequest request) {
    Step step = new Step().fromJson(request.bodyAsJson());
    String validationResponse = step.validateScriptAsXml();
    if (validationResponse.equals("OK")) {
      return storeEntityRespondWith201(request, step);
    } else {
      return Future.failedFuture(validationResponse);
    }
  }

  private Future<Void> putStep(ServiceRequest request) {
    Step step = new Step().fromJson(request.bodyAsJson());
    String validationResponse = step.validateScriptAsXml();
    if (validationResponse.equals("OK")) {
      UUID id = UUID.fromString(request.requestParam("id"));
      return request.moduleStorageAccess().updateEntity(id, step.withUpdatingUser(request.currentUser()))
          .onSuccess(result -> {
            if (result.rowCount() == 1) {
              responseText(request.routingContext(), 204).end();
            } else {
              responseText(request.routingContext(), 404).end("Not found");
            }
          }).mapEmpty();
    } else {
      return Future.failedFuture(validationResponse);
    }
  }

  private Future<Void> getSteps(ServiceRequest request) {
    return getEntities(request, new Step());
  }

  private Future<Void> getStepById(ServiceRequest request) {
    return getEntity(request, new Step());
  }

  private Future<Void> deleteStep(ServiceRequest request) {
    return deleteEntity(request, new Step());
  }

  private Future<Void> getScript(ServiceRequest request) {

    return request.moduleStorageAccess().getScript(request)
        .onSuccess(script -> responseText(request.routingContext(), 200).end(script)).mapEmpty();
  }

  private Future<Void> putScript(ServiceRequest request) {
    String validationResponse = Step.validateScriptAsXml(request.bodyAsString());
    if (validationResponse.equals("OK")) {
      return request.moduleStorageAccess().putScript(request)
          .onSuccess(script -> responseText(request.routingContext(), 204).end()).mapEmpty();
    } else {
      return Future.failedFuture(validationResponse);
    }
  }

  private Future<Void> postTransformation(ServiceRequest request) {
    Transformation transformation = new Transformation().fromJson(request.bodyAsJson());
    return request.moduleStorageAccess().storeEntity(transformation.withCreatingUser(request.currentUser()))
        .compose(transformationId ->
            request.moduleStorageAccess().storeEntities(transformation.getListOfTransformationSteps()))
        .onSuccess(res -> responseText(request.routingContext(), 201).end(transformation.asJson().encodePrettily()));
  }

  private Future<Void> getTransformationById(ServiceRequest request) {
    return getEntity(request, new Transformation());
  }

  private Future<Void> getTransformations(ServiceRequest request) {
    return getEntities(request, new Transformation());
  }

  private Future<Void> updateTransformation(ServiceRequest request) {
    Transformation transformation = new Transformation().fromJson(request.bodyAsJson());
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.moduleStorageAccess().updateEntity(id, transformation.withUpdatingUser(request.currentUser()))
        .onSuccess(result -> {
          if (result.rowCount() == 1) {
            if (transformation.containsListOfSteps()) {
              new TransformationStep().deleteStepsOfATransformation(request, transformation.getRecord().id())
                  .compose(ignore ->
                      request.moduleStorageAccess().storeEntities(transformation.getListOfTransformationSteps()))
                  .onSuccess(res -> responseText(request.routingContext(), 204).end());
            } else {
              responseText(request.routingContext(), 204).end();
            }
          } else {
            responseText(request.routingContext(), 404).end("Not found");
          }
        }).mapEmpty();
  }

  private Future<Void> deleteTransformation(ServiceRequest request) {
    return deleteEntity(request, new Transformation());
  }

  private Future<Void> postTransformationStep(ServiceRequest request) {
    TransformationStep transformationStep = new TransformationStep().fromJson(request.bodyAsJson());
    return transformationStep.createTsaRepositionSteps(request)
        .onSuccess(result -> responseText(request.routingContext, 201).end());
  }

  private Future<Void> getTransformationStepById(ServiceRequest request) {
    return getEntity(request, new TransformationStep());
  }

  private Future<Void> getTransformationSteps(ServiceRequest request) {
    return getEntities(request, new TransformationStep());
  }

  private Future<Void> putTransformationStep(ServiceRequest request) {
    TransformationStep transformationStep = new TransformationStep().fromJson(request.bodyAsJson());

    UUID id = UUID.fromString(request.requestParam("id"));
    return request.moduleStorageAccess().getEntity(id, transformationStep).compose(existingTsa -> {
      if (existingTsa == null) {
        responseText(request.routingContext, 404).end("Not found");
      } else {
        Integer positionOfExistingTsa = ((TransformationStep) existingTsa).getRecord().position();
        transformationStep.updateTsaRepositionSteps(request, positionOfExistingTsa)
            .onSuccess(result -> responseText(request.routingContext, 204).end());
      }
      return Future.succeededFuture();
    });
  }

  private Future<Void> deleteTransformationStep(ServiceRequest request) {
    UUID id = UUID.fromString(request.requestParam("id"));
    ModuleStorageAccess db = request.moduleStorageAccess();
    return db.getEntity(id, new TransformationStep()).compose(existingTsa -> {
      if (existingTsa == null) {
        responseText(request.routingContext, 404).end("Not found");
      } else {
        Integer positionOfExistingTsa = ((TransformationStep) existingTsa).getRecord().position();
        ((TransformationStep) existingTsa).deleteTsaRepositionSteps(db.getTenantPool(), positionOfExistingTsa)
            .onSuccess(result -> responseText(request.routingContext, 200).end());
      }
      return Future.succeededFuture();
    });
  }

  private Future<Void> uploadXmlSourceFile(ServiceRequest request) {

    final long fileStartTime = System.currentTimeMillis();
    String channelId = request.requestParam("id");
    String fileName = request.queryParam("filename", UUID.randomUUID() + ".xml");
    Buffer xmlContent = Buffer.buffer(request.bodyAsString());

    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext, 404)
            .end("Could not find channel with id or tag [" + channelId + "] to upload file to.").mapEmpty();
      } else if (!channel.getRecord().commission()) {
        return responseText(request.routingContext, 403)
            .end("The channel with id or tag [" + channelId + "] is not ready to accept files.").mapEmpty();
      } else if (channel.isCommissioned()) {
        new FileQueue(request, channel.getId().toString()).addNewFile(fileName, xmlContent);
        return responseText(request.routingContext, 204).end().mapEmpty();
      } else {
        return FileListeners.deployIfNotDeployed(request, channel).onSuccess(ignore -> {
          new FileQueue(request, channel.getId().toString()).addNewFile(fileName, xmlContent);
          responseText(request.routingContext, 200)
              .end("File queued for processing in ms " + (System.currentTimeMillis() - fileStartTime));
        }).mapEmpty();
      }
    }).mapEmpty();
  }

  private Future<Void> deployFileListener(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return deployFileListener(request, channelId)
        .onSuccess(response -> responseText(request.routingContext(), 200).end(response)).mapEmpty();
  }

  private Future<String> deployFileListener(ServiceRequest request, String channelId) {
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return Future.succeededFuture("Could not find channel with id or tag [" + channelId + "] to deploy.");
      } else {
        return FileListeners.deployIfNotDeployed(request, channel);
      }
    });
  }

  private Future<Void> undeployFileListener(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel != null) {
        return FileListeners.undeployIfDeployed(request, channel)
            .onSuccess(response -> responseText(request.routingContext(), 200).end(response)).mapEmpty();
      } else {
        return responseText(request.routingContext, 404)
            .end("Found no channel with tag or id " + channelId + " to undeploy.").mapEmpty();
      }
    });
  }

  private Future<Void> pauseImportJob(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext, 404)
            .end("Found no channel with tag or id " + channelId + " to undeploy.").mapEmpty();
      } else {
        UUID channelUuid = channel.getId();
        if (FileListeners.hasFileListener(request.tenant(), channelUuid.toString())) {
          FileProcessor job = FileListeners.getFileListener(request.tenant(), channelUuid.toString()).getImportJob();
          if (job == null) {
            return responseText(request.routingContext(), 404)
                .end("No current job found for this channel, [" + channelUuid + "].");
          } else {
            if (job.paused()) {
              return responseText(request.routingContext(), 200)
                  .end("The job was already paused for channel [" + channelUuid + "].");
            } else {
              job.pause();
              return responseText(request.routingContext(), 200)
                  .end("Processing paused for channel [" + channelUuid + "].");
            }
          }
        } else {
          return responseText(request.routingContext(), 200)
              .end("Channel is not commissioned [" + channelUuid + "].");
        }
      }
    });
  }

  private Future<Void> resumeImportJob(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext, 404)
            .end("Found no channel with tag or id " + channelId + " to undeploy.").mapEmpty();
      } else {
        UUID channelUuid = channel.getId();
        if (FileListeners.hasFileListener(request.tenant(), channelUuid.toString())) {
          FileProcessor job = FileListeners.getFileListener(request.tenant(), channelUuid.toString()).getImportJob();
          if (job == null) {
            return responseText(request.routingContext(), 404)
                .end("No current job found for this channel, [" + channelUuid + "].");
          } else {
            if (job.paused()) {
              job.resume();
              return responseText(request.routingContext(), 200)
                  .end("Processing resumed for channel [" + channelId + "].");
            } else {
              return responseText(request.routingContext(), 200)
                  .end("A job is already running in channel [" + channelId + "].");
            }
          }
        } else {
          return responseText(request.routingContext(), 200)
              .end("Channel is not commissioned [" + channelUuid + "].");
        }
      }
    });
  }

  private Future<Void> initFileSystemQueue(ServiceRequest request) {
    String channelId = request.requestParam("id");
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel != null) {
        String initMessage = new FileQueue(request, channelId).initializeQueue();
        return responseText(request.routingContext(), 200).end(initMessage).mapEmpty();
      } else {
        return responseText(request.routingContext(), 404)
            .end("Could not find channel [" + channelId + "].").mapEmpty();
      }
    });
  }
}
