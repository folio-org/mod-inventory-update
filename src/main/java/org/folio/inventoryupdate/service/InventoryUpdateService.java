package org.folio.inventoryupdate.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.contract.OpenAPIContract;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.UpdateRequest;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;

import java.util.function.Consumer;

import static org.folio.inventoryupdate.ErrorReport.NOT_FOUND;
import static org.folio.inventoryupdate.InventoryUpdateOutcome.OK;
import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;

public class InventoryUpdateService implements RouterCreator, TenantInitHooks {

  public static final Logger logger = LogManager.getLogger("inventory-update");
  public static final String HEALTH_CHECK = "/admin/health";

  HandlersUpdating handlersUpdating = new HandlersUpdating();
  HandlersFetching handlersFetching = new HandlersFetching();


  @Override
  public Future<Router> createRouter(Vertx vertx) {
    return OpenAPIContract.from(vertx, "openapi/inventory-update-5.0.yaml")
        .map(contract -> {
          RouterBuilder routerBuilder = RouterBuilder.create(vertx, contract);
          handlers(vertx, routerBuilder);
          Router router = routerBuilder.createRouter();
          router.route(HEALTH_CHECK).handler(this::handleHealthCheck);
          router.route("/*").handler(this::handleUnrecognizedPath);
          return router;
        }).onSuccess(res -> logger.info("OpenAPI contract parsed."));
  }

  private void handlers(Vertx vertx, RouterBuilder routerBuilder) {
    validatingHandler(vertx, routerBuilder, "singleRecordUpsertByHrid", handlersUpdating::handleInventoryUpsertByHRID);
    validatingHandler(vertx, routerBuilder, "batchUpsertByHrid", handlersUpdating::handleInventoryUpsertByHRIDBatch);
    validatingHandler(vertx, routerBuilder, "deleteInstanceByHrid", handlersUpdating::handleInventoryRecordSetDeleteByHRID);
    validatingHandler(vertx, routerBuilder, "getInventoryRecordSet", handlersFetching::handleInventoryRecordSetFetchHrid);
    // Shared index (decommissioned)
    validatingHandler(vertx, routerBuilder, "upsertByMatchkey", handlersUpdating::handleSharedInventoryUpsertByMatchKey);
    validatingHandler(vertx, routerBuilder, "batchUpsertByMatchkey", handlersUpdating::handleSharedInventoryUpsertByMatchKeyBatch);
    validatingHandler(vertx, routerBuilder, "getSharedInstance", handlersFetching::handleSharedInventoryRecordSetFetch);
    validatingHandler(vertx, routerBuilder, "sharedIndexDeletion", handlersUpdating::handleSharedInventoryRecordSetDeleteByIdentifiers);
  }

  private void validatingHandler(Vertx vertx, RouterBuilder routerBuilder, String operation,
                                 Consumer<UpdateRequest> method) {
    routerBuilder.getRoute(operation)
        .addHandler(ctx -> {
          try {
            method.accept(new RequestValidated(vertx, ctx));
          } catch (RuntimeException e) {
            logger.error("Handler exception {}: {}", operation, e.getMessage(), e);
            exceptionResponse(e, ctx);
          }
        })
        .addFailureHandler(this::routerExceptionResponse); // Open API validation exception
  }


  // UTILS
  public void handleHealthCheck(RoutingContext routingContext) {
    responseJson(routingContext, OK).end("{ \"status\": \"UP\" }");
  }

  public void handleUnrecognizedPath(RoutingContext routingContext) {
    responseError(routingContext, NOT_FOUND, "No Service found for requested path " + routingContext.request().path());
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
    responseError(ctx, ctx.statusCode(), message + ": " + (ctx.failure().getCause() != null ? ctx.failure().getCause().getMessage() : ""));
  }

}
