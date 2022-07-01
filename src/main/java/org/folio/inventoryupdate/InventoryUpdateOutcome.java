package org.folio.inventoryupdate;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.List;

import static org.folio.okapi.common.HttpResponse.responseJson;

public class InventoryUpdateOutcome {
  public static final int OK = 200;
  public static final int MULTI_STATUS = 207;

  public static final String METRICS = "metrics";
  public static final String ERRORS = "errors";
  int statusCode;
  ErrorReport error;
  JsonObject result = new JsonObject();
  List<ErrorReport> errors = new ArrayList<>();
  UpdateMetrics metrics;
  public InventoryUpdateOutcome() {

  }

  public InventoryUpdateOutcome(ErrorReport error) {
    this.statusCode = error.statusCode;
    this.error = error;
  }

  public InventoryUpdateOutcome (JsonObject result) {
    this.result = result;
    if (result.containsKey(ERRORS) && result.getValue(ERRORS) instanceof JsonArray) {

      for (Object o : result.getJsonArray(ERRORS)) {
        errors.add(ErrorReport.makeErrorReportFromJsonString(((JsonObject) o).encode()));
      }
    }
    if (result.containsKey(METRICS) && result.getValue(METRICS) instanceof JsonObject) {
      metrics = UpdateMetrics.makeMetricsFromJson(result.getJsonObject(METRICS));
    }
  }

  public InventoryUpdateOutcome setMetrics (UpdateMetrics metrics) {
    this.metrics = metrics;
    getJson().put(METRICS,metrics.asJson());
    return this;
  }

  public boolean hasMetrics () {
    return metrics != null;
  }

  public boolean hasErrors () {
    return errors != null && ! errors.isEmpty();
  }

  public List<ErrorReport> getErrors () {
    return errors;
  }

  public InventoryUpdateOutcome setErrors (JsonArray errors) {
    getJson().put(ERRORS, errors);
    return this;
  }

  public JsonArray getErrorsAsJsonArray () {
    JsonArray array = new JsonArray();
    for (ErrorReport error : errors) {
      array.add(error.asJson());
    }
    return array;
  }

  public InventoryUpdateOutcome setResponseStatusCode (int status) {
    statusCode = status;
    return this;
  }


  public JsonObject getJson() {
    return result;
  }

  public boolean isError () {
    return error != null;
  }

  public ErrorReport getErrorResponse() {
    return error;
  }

  public boolean failed () {
    return isError();
  }

  public void respond (RoutingContext routingContext) {
    if (statusCode == OK || statusCode == MULTI_STATUS) {
      responseJson(routingContext, statusCode).end(result.encodePrettily());
    } else {
      getErrorResponse().respond(routingContext);
    }
  }
}
