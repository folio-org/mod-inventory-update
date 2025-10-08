package org.folio.inventoryupdate;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.impl.BodyHandlerImpl;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.contract.OpenAPIContract;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.entities.RecordIdentifiers;
import org.folio.inventoryupdate.instructions.ProcessingInstructionsDeletion;
import org.folio.okapi.common.OkapiClient;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;

import java.util.function.Consumer;

import static org.folio.inventoryupdate.ErrorReport.BAD_REQUEST;
import static org.folio.inventoryupdate.ErrorReport.INTERNAL_SERVER_ERROR;
import static org.folio.inventoryupdate.ErrorReport.NOT_FOUND;
import static org.folio.inventoryupdate.InventoryStorage.getOkapiClient;
import static org.folio.inventoryupdate.InventoryUpdateOutcome.MULTI_STATUS;
import static org.folio.inventoryupdate.InventoryUpdateOutcome.OK;
import static org.folio.okapi.common.HttpResponse.responseError;
import static org.folio.okapi.common.HttpResponse.responseJson;

public class NewInventoryUpdateService implements RouterCreator, TenantInitHooks {

  public static final Logger logger = LogManager.getLogger("inventory-import");

  NewInventoryFetchService fetchService = new NewInventoryFetchService();

  @Override
  public Future<Router> createRouter(Vertx vertx) {
    return OpenAPIContract.from(vertx, "openapi/inventory-update-5.0.yaml")
        .map(contract -> {
          RouterBuilder routerBuilder = RouterBuilder.create(vertx, contract);
          handlers(vertx, routerBuilder);
          return routerBuilder.createRouter();
        }).onSuccess(res -> logger.info("OpenAPI contract parsed OK"));

  }


  private void handlers(Vertx vertx, RouterBuilder routerBuilder) {
    validatingHandler(vertx, routerBuilder, "singleRecordUpsertByHrid", this::handleInventoryUpsertByHRID);
    validatingHandler(vertx, routerBuilder, "batchUpsertByHrid", this::handleInventoryUpsertByHRIDBatch);
    validatingHandler(vertx, routerBuilder, "deleteInstanceByHrid", this::handleInventoryRecordSetDeleteByHRID);
    validatingHandler(vertx, routerBuilder, "getInventoryRecordSet", fetchService::handleInventoryRecordSetFetchHrid);
    // Shared index (decommissioned)
    validatingHandler(vertx, routerBuilder, "upsertByMatchkey", this::handleSharedInventoryUpsertByMatchKey);
    validatingHandler(vertx, routerBuilder, "batchUpsertByMatchkey", this::handleSharedInventoryUpsertByMatchKeyBatch);
    validatingHandler(vertx, routerBuilder, "getSharedInstance", fetchService::handleSharedInventoryRecordSetFetch);
    validatingHandler(vertx, routerBuilder, "sharedIndexDeletion", this::handleSharedInventoryRecordSetDeleteByIdentifiers);
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

  public void handleInventoryUpsertByHRID(UpdateRequest updateRequest) {
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    doUpsert(updateRequest, plan);
  }

  public void handleInventoryUpsertByHRIDBatch(UpdateRequest request) {
    System.out.println("In handleInventoryUpsertByHRIDBatch");
    UpdatePlan plan = new UpdatePlanAllHRIDs();
    doBatchUpsert(request, plan);
  }

  public void handleSharedInventoryUpsertByMatchKey(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanSharedInventory();
    doUpsert(request, plan);
  }

  public void handleSharedInventoryUpsertByMatchKeyBatch(UpdateRequest request) {
    UpdatePlan plan = new UpdatePlanSharedInventory();
    doBatchUpsert(request, plan);
  }

  /**
   * Validates a single incoming record set and performs an upsert
   * @param plan a shared-inventory/matchKey, or an inventory/hrid upsert plan.
   */
  private Void doUpsert(UpdateRequest request, UpdatePlan plan) {
    JsonArray inventoryRecordSets = new JsonArray();
    inventoryRecordSets.add(request.bodyAsJson());
    plan.upsertBatch(request.routingContext(), inventoryRecordSets).onComplete(update ->{
      if (update.succeeded()) {
        if (update.result().statusCode == OK || update.result().statusCode == MULTI_STATUS) {
          responseJson(request.routingContext(), update.result().statusCode).end(update.result().getJson().encodePrettily());
        } else {
          update.result().getErrorResponse().respond(request.routingContext());
        }
      }
    });
    return null;
  }

  /**
   * Validates a batch of incoming record sets and performs a batch-upsert
   * @param plan a shared-inventory/matchKey, or an inventory/hrid upsert plan.
   */
  private void doBatchUpsert(UpdateRequest updateRequest, UpdatePlan plan) {
    JsonArray inventoryRecordSets = updateRequest.bodyAsJson().getJsonArray("inventoryRecordSets");
    plan.upsertBatch(updateRequest.routingContext(), inventoryRecordSets).onComplete(update -> {
      // The upsert could succeed, but with an error report, if it was a batch of one
      // Only if a true batch upsert (of more than one) failed, will the promise fail.
      if (update.succeeded()) {
        update.result().respond(updateRequest.routingContext());
      } else {
        logger.error("A batch upsert failed, bringing down all records of the batch. Switching to record-by-record updates");
        UpdateMetrics accumulatedStats = new UpdateMetrics();
        JsonArray accumulatedErrorReport = new JsonArray();
        InventoryUpdateOutcome compositeOutcome = new InventoryUpdateOutcome();
        plan.multipleSingleRecordUpserts(updateRequest.routingContext(), inventoryRecordSets).onComplete(
            listOfOutcomes -> {
              for (InventoryUpdateOutcome outcome : listOfOutcomes.result()) {
                if (outcome.hasMetrics()) {
                  accumulatedStats.add(outcome.metrics);
                }
                if (outcome.hasError()) {
                  accumulatedErrorReport.add(outcome.getError().asJson());
                }
              }
              compositeOutcome.setMetrics(accumulatedStats);
              compositeOutcome.setErrors(accumulatedErrorReport);
              compositeOutcome.setResponseStatusCode(accumulatedErrorReport.isEmpty() ? OK : MULTI_STATUS);
              compositeOutcome.respond(updateRequest.routingContext());
            });
      }
    });
  }

  // DELETE REQUESTS
  public void handleInventoryRecordSetDeleteByHRID(UpdateRequest request) {
    ProcessingInstructionsDeletion deleteInstructions =  new ProcessingInstructionsDeletion(request.bodyAsJson().getJsonObject("processing"));

    InventoryQuery queryByInstanceHrid = new QueryByHrid(request.bodyAsJson().getString("hrid"));
    DeletePlan deletePlan = DeletePlanAllHRIDs.getDeletionPlan(queryByInstanceHrid);
    runDeletionPlan(deletePlan, deleteInstructions, request.routingContext());
  }

  public void handleSharedInventoryRecordSetDeleteByIdentifiers(UpdateRequest request) {
    InventoryUpdateOutcome isJsonContentType = contentTypeIsJson(request.routingContext());
    if (isJsonContentType.failed()) {
      isJsonContentType.getErrorResponse().respond(request.routingContext());
      return;
    }

    InventoryUpdateOutcome incomingValidJson = getIncomingJsonBody(request);
    if (incomingValidJson.failed()) {
      incomingValidJson.getErrorResponse().respond(request.routingContext());
      return;
    }
    JsonObject processing = incomingValidJson.getJson().getJsonObject( "processing" );
    ProcessingInstructionsDeletion deleteInstructions =  new ProcessingInstructionsDeletion(processing);

    RecordIdentifiers deletionIdentifiers = RecordIdentifiers.identifiersFromDeleteRequestJson(incomingValidJson.getJson());
    DeletePlan deletePlan = DeletePlanSharedInventory.getDeletionPlan(deletionIdentifiers);
    runDeletionPlan(deletePlan, deleteInstructions, request.routingContext());
  }

  private void runDeletionPlan(DeletePlan deletePlan, ProcessingInstructionsDeletion deleteInstructions, RoutingContext routingCtx) {

    OkapiClient okapiClient = getOkapiClient(routingCtx);
    deletePlan.planInventoryDelete(okapiClient, deleteInstructions).onComplete(planDone -> {
      if (planDone.succeeded()) {
        deletePlan.doInventoryDelete(okapiClient).onComplete(deletionsDone -> {
          JsonObject response = new JsonObject();
          response.put("metrics", deletePlan.getUpdateStats());
          if (deletionsDone.succeeded()) {
            respondWithOK(routingCtx,response);
          } else {
            response.put("errors", deletePlan.getErrors());
            response.getJsonArray("errors")
                .add(new ErrorReport(
                    ErrorReport.ErrorCategory.STORAGE,
                    INTERNAL_SERVER_ERROR,
                    deletionsDone.cause().getMessage())
                    .asJson());
            respondWithMultiStatus(routingCtx,response);
          }
        });
      }  else {
        if (!deletePlan.foundExistingRecordSet()) {
          new ErrorReport(
              ErrorReport.ErrorCategory.STORAGE,
              NOT_FOUND,
              "Error processing delete request: "+ planDone.cause().getMessage())
              .respond(routingCtx);
        } else {
          new ErrorReport(
              ErrorReport.ErrorCategory.STORAGE,
              INTERNAL_SERVER_ERROR,
              planDone.cause().getMessage())
              .respond(routingCtx);
        }
      }
    });
  }

  // UTILS
  public void handleHealthCheck(RoutingContext routingContext) {
    responseJson(routingContext, OK).end("{ \"status\": \"UP\" }");
  }

  public void handleUnrecognizedPath(RoutingContext routingContext) {
    responseError(routingContext, NOT_FOUND, "No Service found for requested path " + routingContext.request().path());
  }

  private InventoryUpdateOutcome contentTypeIsJson (RoutingContext routingCtx) {
    String contentType = routingCtx.request().getHeader("Content-Type");
    if (contentType != null && !contentType.startsWith("application/json")) {
      logger.error("Only accepts Content-Type application/json, was: {}", contentType);
      return new InventoryUpdateOutcome(new ErrorReport(
          ErrorReport.ErrorCategory.VALIDATION,
          BAD_REQUEST,
          "Only accepts Content-Type application/json, content type was: "+ contentType)
          .setShortMessage("Only accepts application/json")
          .setStatusCode(BAD_REQUEST));
    } else {
      return new InventoryUpdateOutcome(new JsonObject().put("succeeded", "yes"));
    }
  }

  private InventoryUpdateOutcome getIncomingJsonBody(UpdateRequest request) {
    System.out.println("Get incoming JSON body");
    String bodyAsString;
    try {
      bodyAsString = request.bodyAsJson().toString();
      System.out.println(bodyAsString);
      if (bodyAsString == null) {
        return new InventoryUpdateOutcome(
            new ErrorReport(
                ErrorReport.ErrorCategory.VALIDATION,
                BAD_REQUEST,
                "No request body provided."));
      }
      JsonObject bodyAsJson = new JsonObject(bodyAsString);
      logger.debug("Request body {}", bodyAsJson::encodePrettily);
      return new InventoryUpdateOutcome(bodyAsJson);
    } catch (DecodeException de) {
      return new InventoryUpdateOutcome(new ErrorReport(ErrorReport.ErrorCategory.VALIDATION,
          BAD_REQUEST,
          "Could not parse JSON body of the request: " + de.getMessage())
          .setShortMessage("Could not parse request JSON"));
    }
  }

  public void respondWithOK(RoutingContext routingContext, JsonObject message) {
    responseJson(routingContext, OK).end(message.encodePrettily());
  }

  public void respondWithMultiStatus(RoutingContext routingContext, JsonObject message) {
    responseJson(routingContext, MULTI_STATUS).end(message.encodePrettily());
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

}
