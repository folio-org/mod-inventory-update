package org.folio.inventoryimport.service;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import io.vertx.ext.web.handler.impl.BodyHandlerImpl;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.contract.OpenAPIContract;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryimport.foliodata.ConfigurationsClient;
import org.folio.inventoryimport.foliodata.SettingsClient;
import org.folio.inventoryimport.moduledata.*;
import org.folio.inventoryimport.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryimport.moduledata.database.SqlQuery;
import org.folio.inventoryimport.moduledata.database.Tables;
import org.folio.inventoryimport.service.fileimport.FileProcessor;
import org.folio.inventoryimport.service.fileimport.FileQueue;
import org.folio.inventoryimport.service.fileimport.FileListeners;
import org.folio.inventoryimport.service.fileimport.XmlFileListener;
import org.folio.inventoryimport.utils.Miscellaneous;
import org.folio.inventoryimport.utils.SettableClock;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;
import org.folio.tlib.postgres.PgCqlException;

import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static org.folio.okapi.common.HttpResponse.*;

/**
 * Main service.
 */
public class ImportService implements RouterCreator, TenantInitHooks {

    public static final Logger logger = LogManager.getLogger("inventory-import");

    @Override
    public Future<Router> createRouter(Vertx vertx) {
        return OpenAPIContract.from(vertx, "openapi/inventory-import-1.0.yaml")
            .map(contract -> {
                RouterBuilder routerBuilder = RouterBuilder.create(vertx, contract);
                handlers(vertx, routerBuilder);
                return routerBuilder.createRouter();
            }).onSuccess(res -> logger.info("OpenAPI parsed OK"));
    }

    private void handlers(Vertx vertx, RouterBuilder routerBuilder) {
        // Configurations
        validatingHandler(vertx, routerBuilder, "postImportConfig", this::postImportConfig);
        validatingHandler(vertx, routerBuilder, "getImportConfigs", this::getImportConfigs);
        validatingHandler(vertx, routerBuilder, "getImportConfig", this::getImportConfigById);
        validatingHandler(vertx, routerBuilder, "putImportConfig", this::putImportConfig);
        validatingHandler(vertx, routerBuilder, "deleteImportConfig", this::deleteImportConfig);
        validatingHandler(vertx, routerBuilder, "postTransformation", this::postTransformation);
        validatingHandler(vertx, routerBuilder, "getTransformation", this::getTransformationById);
        validatingHandler(vertx, routerBuilder, "getTransformations", this::getTransformations);
        validatingHandler(vertx, routerBuilder, "putTransformation", this::putTransformation);
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
        validatingHandler(vertx, routerBuilder, "getFailedRecordsForJob", this::getFailedRecords);
        validatingHandler(vertx, routerBuilder, "postFailedRecordsForJob", this::postFailedRecordsForJob);
        validatingHandler(vertx, routerBuilder, "deleteRecordFailure", this::deleteRecordFailure);
        // Processing
        validatingHandler(vertx, routerBuilder, "purgeAgedLogs", this::purgeAgedLogs);
        nonValidatingHandler(vertx, routerBuilder, "importXmlRecords", this::stageXmlSourceFile);
        validatingHandler(vertx, routerBuilder, "startFileListener", this::activateFileListener);
        validatingHandler(vertx, routerBuilder, "pauseImport", this::pauseImportJob);
        validatingHandler(vertx, routerBuilder, "resumeImport", this::resumeImportJob);
    }

    private void validatingHandler(Vertx vertx, RouterBuilder routerBuilder, String operation,
                                   Function<ServiceRequest, Future<Void>> method) {
        routerBuilder.getRoute(operation)
            .addHandler(ctx -> {
                try {
                    method.apply(new RequestValidated(vertx, ctx))
                        .onFailure(cause -> {
                            logger.error("Handler failure {}: {}", operation, cause.getMessage());
                            exceptionResponse(cause, ctx);
                        });
                } catch (Exception e) {
                    logger.error("Handler exception {}: {}", operation, e.getMessage(), e);
                    exceptionResponse(e, ctx);
                }
            })
            .addFailureHandler(this::routerExceptionResponse);
    }

    /**
     * For POSTing text, PUTting xml, decoding the CQL query parameter
     */
    private void nonValidatingHandler(Vertx vertx, RouterBuilder routerBuilder, String operation,
                                      Function<ServiceRequest, Future<Void>> method) {
        routerBuilder.getRoute(operation)
            .addHandler(new BodyHandlerImpl())
            .setDoValidation(false)
            .addHandler(ctx -> {
                try {
                    method.apply(new RequestUnvalidated(vertx, ctx))
                        .onFailure(cause -> {
                            logger.error("Non-validating handler failure {}: {}", operation, cause.getMessage());
                            exceptionResponse(cause, ctx);
                        });
                } catch (Exception e) {  // exception thrown by method
                    logger.error("Non-validating handler exception {}: {}", operation, e.getMessage(), e);
                    exceptionResponse(e, ctx);
                }
            })
            .addFailureHandler(this::routerExceptionResponse);
    }



    private void exceptionResponse(Throwable cause, RoutingContext routingContext) {
        if (cause.getMessage().toLowerCase().contains("could not find")) {
            responseError(routingContext, 404, cause.getMessage());
        } else {
            responseError(routingContext, 400, cause.getClass().getSimpleName() + ": " + cause.getMessage());
        }
    }

    /**
     * Returns request validation exception, potentially with improved error message if problem was
     * an error in a polymorph schema, like in `harvestable` of type `oaiPmh` vs `xmlBulk`.
     */
    private void routerExceptionResponse(RoutingContext ctx) {
        String message = null;
        if (ctx.failure() != null) message = ctx.failure().getMessage();
        responseError(ctx, ctx.statusCode(), message + ": " + ctx.failure().getCause().getMessage());

    }

    @Override
    public Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
        return new ModuleStorageAccess(vertx, tenant).init(tenantAttributes)
                .onFailure(x -> logger.error("Database initialization failed: " + x.getMessage()))
                .onSuccess(x -> logger.info("Tenant '" + tenant + "' database initialized"));
    }

    private Future<Void> getEntities(ServiceRequest request, Entity entity) {
        ModuleStorageAccess db = request.moduleStorageAccess();
        SqlQuery query;
        try {
            query = entity
                    .makeSqlFromCqlQuery(request, db.schemaDotTable(entity.table()));
        } catch (PgCqlException pce) {
            responseText(request.routingContext, 400)
                    .end("Could not execute query to retrieve " + entity.jsonCollectionName() + ": " + pce.getMessage() + " Request:" + request.absoluteURI());
            return Future.succeededFuture();
        } catch (Exception e) {
            return Future.failedFuture(e.getMessage());
        }
        return db.getEntities(query.getQueryWithLimits(), entity).onComplete(
                result -> {
                    if (result.succeeded()) {
                        JsonObject responseJson = new JsonObject();
                        JsonArray jsonRecords = new JsonArray();
                        responseJson.put(entity.jsonCollectionName(), jsonRecords);
                        List<Entity> recs = result.result();
                        for (Entity rec : recs) {
                            jsonRecords.add(rec.asJson());
                        }
                        db.getCount(query.getCountingSql()).onComplete(
                                count -> {
                                    responseJson.put("totalRecords", count.result());
                                    responseJson(request.routingContext, 200).end(responseJson.encodePrettily());
                                }
                        );
                    } else {
                        responseText(request.routingContext, 500)
                                .end("Problem retrieving jobs: " + result.cause().getMessage());
                    }
                }
        ).mapEmpty();
    }

    private Future<Void> getEntity(ServiceRequest request, Entity entity) {
        UUID id = UUID.fromString(request.requestParam("id"));
        return request.moduleStorageAccess().getEntity(id, entity)
                .onSuccess(instance -> {
                    if (instance == null) {
                        responseText(request.routingContext, 404).end(entity.entityName() + " " + id + " not found.");
                    } else {
                        responseJson(request.routingContext, 200).end(instance.asJson().encodePrettily());
                    }
                })
                .mapEmpty();
    }

    private Future<Void> deleteEntity(ServiceRequest request, Entity entity) {
        UUID id = UUID.fromString(request.requestParam("id"));
        return request.moduleStorageAccess().deleteEntity(id, entity).
                onSuccess(result -> {
                    if (result == 0) {
                        responseText(request.routingContext, 404).end("Not found");
                    } else {
                        responseText(request.routingContext, 200).end();
                    }
                }).mapEmpty();
    }

    private Future<Void> storeEntityRespondWith201(ServiceRequest request, Entity entity) {
        ModuleStorageAccess db = request.moduleStorageAccess();
        return db.storeEntity(entity)
                .onSuccess(
                        id -> db.getEntity(id, entity)
                                .map(stored -> responseJson(request.routingContext, 201)
                                        .end(stored.asJson().encodePrettily())))
                .mapEmpty();
    }

    private Future<Void> postImportConfig(ServiceRequest request) {
        ImportConfig importConfig = new ImportConfig().fromJson(request.bodyAsJson());
        return storeEntityRespondWith201(request, importConfig);
    }

    private Future<Void> getImportConfigs(ServiceRequest request) {
        return getEntities(request, new ImportConfig());
    }

    private Future<Void> getImportConfigById(ServiceRequest request) {
        return getEntity(request, new ImportConfig());
    }

    private Future<Void> putImportConfig(ServiceRequest request) {
        ImportConfig importConfig = new ImportConfig().fromJson(request.bodyAsJson());
        UUID id = UUID.fromString(request.requestParam("id"));
        return request.moduleStorageAccess().updateEntity(id,importConfig)
                .onSuccess(result-> {
                    if (result.rowCount()==1) {
                        responseText(request.routingContext(), 204).end();
                    } else {
                        responseText(request.routingContext(), 404).end("Not found");
                    }
                }).mapEmpty();
    }

    private Future<Void> deleteImportConfig(ServiceRequest request) {
        return deleteEntity(request, new ImportConfig());
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
                    .makeSqlFromCqlQuery(request, db.schemaDotTable(Tables.import_job))
                    .withAdditionalWhereClause(timeRange);
        } catch (PgCqlException pce) {
            responseText(request.routingContext(), 400)
                    .end("Could not execute query to retrieve jobs: " + pce.getMessage() + " Request:" + request.absoluteURI());
            return Future.succeededFuture();
        } catch (Exception e) {
            return Future.failedFuture(e.getMessage());
        }
        return db.getEntities(query.getQueryWithLimits(), new ImportJob()).onComplete(
                jobsList -> {
                    if (jobsList.succeeded()) {
                        JsonObject responseJson = new JsonObject();
                        JsonArray importJobs = new JsonArray();
                        responseJson.put("importJobs", importJobs);
                        List<Entity> jobs = jobsList.result();
                        for (Entity job : jobs) {
                            importJobs.add(job.asJson());
                        }
                        db.getCount(query.getCountingSql()).onComplete(
                                count -> {
                                    responseJson.put("totalRecords", count.result());
                                    responseJson(request.routingContext(), 200).end(responseJson.encodePrettily());
                                }
                        );
                    } else {
                        responseText(request.routingContext(), 500)
                                .end("Problem retrieving jobs: " + jobsList.cause().getMessage());
                    }
                }
        ).mapEmpty();
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
            logLines.add(new LogLine().fromJson((JsonObject) o));
        }
        return request.moduleStorageAccess()
                .storeEntities(new LogLine(), logLines)
                .onSuccess(configId ->
                        responseJson(request.routingContext(), 201).end(logLines.size() + " log line(s) created."))
                .mapEmpty();
    }

    private Future<Void> getLogStatements(ServiceRequest request) {
        return getEntities(request, new LogLine());
    }

    private Future<Void> getFailedRecords(ServiceRequest request) {

        ModuleStorageAccess db = request.moduleStorageAccess();
        SqlQuery queryFromCql = new RecordFailure().makeSqlFromCqlQuery(
                        request, db.schemaDotTable(Tables.record_failure_view))
                .withDefaultLimit("100");
        String jobId = request.requestParam("id");
        String from = request.queryParam("from");
        String until = request.queryParam("until");

        String timeRange = null;
        if (from != null && until != null) {
            timeRange = " (time_stamp >= '" + from
                    + "'  AND time_stamp <= '" + until + "') ";
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

        return db.getEntities(queryFromCql.getQueryWithLimits(), new RecordFailure()).onComplete(
                failuresList -> {
                    if (failuresList.succeeded()) {
                        JsonObject responseJson = new JsonObject();
                        JsonArray recordFailures = new JsonArray();
                        responseJson.put("failedRecords", recordFailures);
                        List<Entity> failures = failuresList.result();
                        for (Entity failure : failures) {
                            recordFailures.add(failure.asJson());
                        }
                        db.getCount(queryFromCql.getCountingSql()).onComplete(
                                count -> {
                                    responseJson.put("totalRecords", count.result());
                                    responseJson(request.routingContext(), 200).end(responseJson.encodePrettily());
                                }
                        );
                    }
                }
        ).mapEmpty();
    }


    private Future<Void> postFailedRecordsForJob(ServiceRequest request) {
        JsonArray recs = request.bodyAsJson().getJsonArray(new RecordFailure().jsonCollectionName());
        List<Entity> failedRecs = new ArrayList<>();
        for (Object o : recs) {
            failedRecs.add(new RecordFailure().fromJson((JsonObject) o));
        }
        return request.moduleStorageAccess()
                .storeEntities(new RecordFailure(), failedRecs)
                .onSuccess(configId ->
                        responseJson(request.routingContext(), 201).end(failedRecs.size() + " record failures logged."))
                .mapEmpty();
    }

    private Future<Void> deleteRecordFailure(ServiceRequest request) {
        return deleteEntity(request, new RecordFailure());
    }

    private Future<Void> purgeAgedLogs(ServiceRequest request) {
        logger.info("Running timer process: purge aged logs");
        final String SETTINGS_SCOPE = "mod-inventory-import";
        final String SETTINGS_KEY = "PURGE_LOGS_AFTER";
        SettingsClient.getStringValue(request.routingContext(),
                        SETTINGS_SCOPE,
                        SETTINGS_KEY)
                .onComplete(settingsValue -> {
                    if (settingsValue.result() != null) {
                        applyPurgeOfPastJobs(request, settingsValue.result());
                    } else {
                        final String CONFIGS_MODULE = "mod-inventory-import";
                        final String CONFIGS_CONFIG_NAME = "PURGE_LOGS_AFTER";
                        ConfigurationsClient.getStringValue(request.routingContext(),
                                        CONFIGS_MODULE,
                                        CONFIGS_CONFIG_NAME)
                                .onComplete(configsValue -> applyPurgeOfPastJobs(request, configsValue.result()));
                    }
                });
        return Future.succeededFuture();
    }

    private void applyPurgeOfPastJobs(ServiceRequest request, String purgeSetting) {
        Period ageForDeletion = Miscellaneous.getPeriod(purgeSetting,3, "MONTHS");
        LocalDateTime untilDate = SettableClock.getLocalDateTime().minus(ageForDeletion).truncatedTo(ChronoUnit.MINUTES);
        logger.info("Running timer process: purging aged logs from before " + untilDate);
        request.moduleStorageAccess().purgePreviousJobsByAge(untilDate)
                .onComplete(x -> request.routingContext().response().setStatusCode(204).end()).mapEmpty();
    }


    private Future<Void> postStep(ServiceRequest request) {
        Step step = new Step().fromJson(request.bodyAsJson());
        String validationResponse = step.validateScriptAsXml();
        if (validationResponse.equals("OK")) {
            return storeEntityRespondWith201(request, step);
        }  else {
            return Future.failedFuture(validationResponse);
        }
    }

    private Future<Void> putStep(ServiceRequest request) {
        Step step = new Step().fromJson(request.bodyAsJson());
        String validationResponse = step.validateScriptAsXml();
        if (validationResponse.equals("OK")) {
            UUID id = UUID.fromString(request.requestParam("id"));
            return request.moduleStorageAccess().updateEntity(id,step)
                    .onSuccess(result-> {
                        if (result.rowCount()==1) {
                            responseText(request.routingContext(), 204).end();
                        } else {
                            responseText(request.routingContext(), 404).end("Not found");
                        }
                    }).mapEmpty();
        }  else {
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
                .onSuccess(script -> responseText(request.routingContext(), 200).end(script))
                .mapEmpty();
    }

    private Future<Void> putScript(ServiceRequest request) {
        String validationResponse = Step.validateScriptAsXml(request.bodyAsString());
        if (validationResponse.equals("OK")) {
            return request.moduleStorageAccess().putScript(request)
                    .onSuccess(script -> responseText(request.routingContext(), 204).end())
                    .mapEmpty();
        } else {
            return Future.failedFuture(validationResponse);
        }
    }

    private Future<Void> postTransformation(ServiceRequest request) {
        Entity transformation = new Transformation().fromJson(request.bodyAsJson());
        return storeEntityRespondWith201(request, transformation);
    }

    private Future<Void> getTransformationById(ServiceRequest request) {
        return getEntity(request, new Transformation());
    }

    private Future<Void> getTransformations(ServiceRequest request) {
        return getEntities(request, new Transformation());
    }

    private Future<Void> putTransformation(ServiceRequest request) {
        Transformation transformation = new Transformation().fromJson(request.bodyAsJson());
        UUID id = UUID.fromString(request.requestParam("id"));
        return request.moduleStorageAccess().updateEntity(id, transformation)
                .onSuccess(result-> {
                    if (result.rowCount()==1) {
                        responseText(request.routingContext(), 204).end();
                    } else {
                        responseText(request.routingContext(), 404).end("Not found");
                    }
                }).mapEmpty();
    }

    private Future<Void> deleteTransformation(ServiceRequest request) {
        return deleteEntity(request, new Transformation());
    }

    private Future<Void> postTransformationStep(ServiceRequest request) {
        Entity transformationStep = new TransformationStep().fromJson(request.bodyAsJson());
        return storeEntityRespondWith201(request, transformationStep);
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
        ModuleStorageAccess db = request.moduleStorageAccess();
        return db.getEntity(id, transformationStep)
                .compose(existingTsa -> {
                            if (existingTsa == null) {
                                responseText(request.routingContext, 404).end("Not found");
                            } else {
                                Integer positionOfExistingTsa = ((TransformationStep) existingTsa).record.position();
                                transformationStep.updateTsaReorderSteps(db.getTenantPool(), positionOfExistingTsa)
                                        .onSuccess(result -> responseText(request.routingContext, 204).end());
                            }
                            return Future.succeededFuture();
                        });
    }

    private Future<Void> deleteTransformationStep(ServiceRequest request) {
        return deleteEntity(request, new TransformationStep());
    }

    private Future<Void> stageXmlSourceFile(ServiceRequest request) {

        final long fileStartTime = System.currentTimeMillis();
        String importConfigId = request.requestParam("id");
        String fileName = request.queryParam("filename",UUID.randomUUID() + ".xml");
        Buffer xmlContent = Buffer.buffer(request.bodyAsString());

        return activateFileListener(request, importConfigId)
                .onSuccess(ignore -> {
                    new FileQueue(request, importConfigId).addNewFile(fileName, xmlContent);
                    responseText(request.routingContext, 200).end("File queued for processing in ms " + (System.currentTimeMillis() - fileStartTime));
                }).mapEmpty();
    }

    private Future<Void> activateFileListener(ServiceRequest request) {
        String importConfigId = request.requestParam("id");
        return activateFileListener(request, importConfigId)
                .onSuccess(response -> responseText(request.routingContext(), 200).end(response)).mapEmpty();
    }

    private Future<String> activateFileListener(ServiceRequest request, String importConfigId) {
        Promise<String> promise = Promise.promise();
        request.moduleStorageAccess().getEntity(UUID.fromString(importConfigId), new ImportConfig())
                .onSuccess(cfg -> {
                    if (cfg != null) {
                        XmlFileListener
                                .deployIfNotDeployed(request, importConfigId)
                                .onSuccess(promise::complete);
                    } else {
                        promise.fail("Could not find import config with id [" + importConfigId + "].");
                    }
                }).mapEmpty();
        return promise.future();
    }

    private Future<Void> pauseImportJob(ServiceRequest request) {
        String importConfigId = request.requestParam("id");
        if (FileListeners.hasFileListener(request.tenant(), importConfigId)) {
            FileProcessor job = FileListeners.getFileListener(request.tenant(), importConfigId).getImportJob();
            if (job.paused()) {
                responseText(request.routingContext(), 200).end("File listener already paused for import config [" + importConfigId + "].");
            } else {
                job.pause();
                responseText(request.routingContext(), 200).end("Processing paused for import config [" + importConfigId + "].");
            }
        } else {
            responseText(request.routingContext(), 200).end("Currently no running import process found to pause for import config [" + importConfigId + "].");
        }
        return Future.succeededFuture();
    }

    private Future<Void> resumeImportJob(ServiceRequest request) {

        String importConfigId = request.requestParam("id");
        if (FileListeners.hasFileListener(request.tenant(), importConfigId)) {
            FileProcessor job = FileListeners.getFileListener(request.tenant(), importConfigId).getImportJob();
            if (job.paused()) {
                job.resume();
                responseText(request.routingContext(), 200).end("Processing resumed for import config [" + importConfigId + "].");
            } else {
                responseText(request.routingContext(), 200).end("File listener already active for import config [" + importConfigId + "].");
            }
        } else {
            responseText(request.routingContext(), 200).end("Currently no running import process found to resume for import config [" + importConfigId + "].");
        }
        return Future.succeededFuture();

    }



}
