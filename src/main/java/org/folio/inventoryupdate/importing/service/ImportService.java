package org.folio.inventoryupdate.importing.service;

import static org.folio.inventoryupdate.importing.service.delivery.respond.Channels.getChannelByTagOrUuid;
import static org.folio.okapi.common.HttpResponse.responseError;
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
import java.util.UUID;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileListeners;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.FileQueue;
import org.folio.inventoryupdate.importing.service.delivery.respond.Channels;
import org.folio.inventoryupdate.importing.service.delivery.respond.JobsAndMonitoring;
import org.folio.inventoryupdate.importing.service.delivery.respond.LogPurging;
import org.folio.inventoryupdate.importing.service.delivery.respond.Transformations;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;

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
    validatingHandler(vertx, routerBuilder, "postChannel", Channels::postChannel);
    validatingHandler(vertx, routerBuilder, "getChannels", Channels::getChannels);
    validatingHandler(vertx, routerBuilder, "getChannel", Channels::getChannelById);
    validatingHandler(vertx, routerBuilder, "putChannel", Channels::putChannel);
    validatingHandler(vertx, routerBuilder, "deleteChannel", Channels::deleteChannel);
    validatingHandler(vertx, routerBuilder, "initFileSystemQueue", Channels::initFileSystemQueue);
    validatingHandler(vertx, routerBuilder, "commission", Channels::commission);
    validatingHandler(vertx, routerBuilder, "decommission", Channels::decommission);
    validatingHandler(vertx, routerBuilder, "listen", Channels::listen);
    validatingHandler(vertx, routerBuilder, "noListen", Channels::noListen);

    validatingHandler(vertx, routerBuilder, "postTransformation", Transformations::postTransformation);
    validatingHandler(vertx, routerBuilder, "getTransformation", Transformations::getTransformationById);
    validatingHandler(vertx, routerBuilder, "getTransformations", Transformations::getTransformations);
    validatingHandler(vertx, routerBuilder, "putTransformation", Transformations::updateTransformation);
    validatingHandler(vertx, routerBuilder, "deleteTransformation", Transformations::deleteTransformation);
    validatingHandler(vertx, routerBuilder, "postStep", Transformations::postStep);
    validatingHandler(vertx, routerBuilder, "getSteps", Transformations::getSteps);
    validatingHandler(vertx, routerBuilder, "getStep", Transformations::getStepById);
    validatingHandler(vertx, routerBuilder, "putStep", Transformations::putStep);
    validatingHandler(vertx, routerBuilder, "deleteStep", Transformations::deleteStep);
    validatingHandler(vertx, routerBuilder, "getScript", Transformations::getScript);
    nonValidatingHandler(vertx, routerBuilder, "putScript", Transformations::putScript);
    validatingHandler(vertx, routerBuilder, "postTsa", Transformations::postTransformationStep);
    validatingHandler(vertx, routerBuilder, "getTsas", Transformations::getTransformationSteps);
    validatingHandler(vertx, routerBuilder, "getTsa", Transformations::getTransformationStepById);
    validatingHandler(vertx, routerBuilder, "putTsa", Transformations::putTransformationStep);
    validatingHandler(vertx, routerBuilder, "deleteTsa", Transformations::deleteTransformationStep);

    // Job handling and tracking
    validatingHandler(vertx, routerBuilder, "getImportJobs", JobsAndMonitoring::getImportJobs);
    validatingHandler(vertx, routerBuilder, "getImportJob", JobsAndMonitoring::getImportJobById);
    validatingHandler(vertx, routerBuilder, "postImportJob", JobsAndMonitoring::postImportJob);
    validatingHandler(vertx, routerBuilder, "deleteImportJob", JobsAndMonitoring::deleteImportJob);
    nonValidatingHandler(vertx, routerBuilder, "postImportJobLogLines", JobsAndMonitoring::postLogStatements);
    nonValidatingHandler(vertx, routerBuilder, "getImportJobLogLines", JobsAndMonitoring::getLogStatements);
    validatingHandler(vertx, routerBuilder, "getFailedRecords", JobsAndMonitoring::getFailedRecords);
    validatingHandler(vertx, routerBuilder, "postFailedRecords", JobsAndMonitoring::postFailedRecords);
    validatingHandler(vertx, routerBuilder, "deleteRecordFailure", JobsAndMonitoring::deleteRecordFailure);
    validatingHandler(vertx, routerBuilder, "pauseJob", JobsAndMonitoring::pauseImportJob);
    validatingHandler(vertx, routerBuilder, "resumeJob", JobsAndMonitoring::resumeImportJob);

    // Systems operations
    validatingHandler(vertx, routerBuilder, "purgeAgedLogs", LogPurging::purgeAgedLogs);
    validatingHandler(vertx, routerBuilder, "recoverInterruptedChannels", Channels::recoverChannels);

    // Importing
    nonValidatingHandler(vertx, routerBuilder, "uploadXmlRecords", this::uploadXmlSourceFile);
    // Dry run
    nonValidatingHandler(vertx, routerBuilder, "echoTransformation", Transformations::tryTransformation);
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
    if (routingContext.response().headWritten()) {
      logger.error("Exception: {}  (response already sent)", cause.getMessage());
    } else {
      if (cause.getMessage().toLowerCase().contains("could not find")) {
        responseError(routingContext, 404, cause.getMessage());
      } else {
        responseError(routingContext, 400, cause.getClass().getSimpleName() + ": " + cause.getMessage());
      }
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
    return new EntityStorage(vertx, tenant).init(tenantAttributes).onFailure(x ->
            logger.error("Database initialization failed: {}", x.getMessage())).onSuccess(x ->
            logger.info("Tenant '{}' database initialized", tenant))
        .compose(x ->
            clearTenantFileQueues(vertx, tenant, getTenantParameter(tenantAttributes, "clearPastFileQueues")))
        .compose(na -> FileListeners.clearRegistry());
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

  private Future<Void> uploadXmlSourceFile(ServiceRequest request) {
    final long fileStartTime = System.nanoTime();
    String channelId = request.requestParam("id");
    String fileName = request.queryParam("filename", UUID.randomUUID() + ".xml");
    Buffer xmlContent = Buffer.buffer(request.bodyAsString());

    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext, 404)
            .end("Could not find channel with id or tag [" + channelId + "] to upload file to.").mapEmpty();
      } else if (!channel.isEnabled()) {
        return responseText(request.routingContext, 403)
            .end("The channel with id or tag [" + channelId + "] is not ready to accept files.").mapEmpty();
      } else if (channel.isCommissioned()) {
        new FileQueue(request, channel.getId().toString()).addNewFile(fileName, xmlContent);
        return responseText(request.routingContext, 200).end().mapEmpty();
      } else {
        new FileQueue(request, channel.getId().toString()).addNewFile(fileName, xmlContent);
        return FileListeners.deployIfNotDeployed(request, channel).onSuccess(ignore -> {
          new FileQueue(request, channel.getId().toString()).addNewFile(fileName, xmlContent);
          responseText(request.routingContext, 200)
              .end("File queued for processing in ms " + (System.nanoTime() - fileStartTime) / 1000000L);
        }).mapEmpty();
      }
    });
  }
}
