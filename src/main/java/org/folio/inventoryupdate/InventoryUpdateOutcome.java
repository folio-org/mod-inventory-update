package org.folio.inventoryupdate;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import static org.folio.okapi.common.HttpResponse.responseJson;

public class InventoryUpdateOutcome {
  public static final int OK = 200;
  public static final int MULTI_STATUS = 207;

  int statusCode;
  ErrorReport errors;
  JsonObject goodResult;
  boolean success;
  UpdateMetrics metrics;
  public InventoryUpdateOutcome() {
    success = true;
  }
  public InventoryUpdateOutcome(ErrorReport error) {
    this.statusCode = error.statusCode;
    this.errors = error;
    this.success = false;
  }

  public InventoryUpdateOutcome (JsonObject goodResult) {
    this.goodResult = goodResult;
    success = true;
  }

  public InventoryUpdateOutcome (UpdateMetrics metrics, ErrorReport errors) {
    this.metrics = metrics;
    this.errors = errors;
    this.statusCode = MULTI_STATUS;

  }
  public int getStatus() {
    return statusCode;
  }

  public InventoryUpdateOutcome setResponseStatusCode (int status) {
    statusCode = status;
    return this;
  }

  public ErrorReport getErrorResponse() {
    return errors;
  }


  public JsonObject getJson() {
    return goodResult;
  }

  public boolean succeeded () {
    return success;
  }

  public boolean failed () {
    return !success;
  }

  public void respond (RoutingContext routingContext) {
    if (statusCode == 200 || statusCode == 207) {
      responseJson(routingContext, statusCode).end(goodResult.encodePrettily());
    } else {
      getErrorResponse().respond(routingContext);
    }
  }
}
