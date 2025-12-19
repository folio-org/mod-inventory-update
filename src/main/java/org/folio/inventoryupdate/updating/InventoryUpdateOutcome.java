package org.folio.inventoryupdate.updating;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.List;

import static org.folio.okapi.common.HttpResponse.responseJson;

public class InventoryUpdateOutcome {
  public static final int OK = 200;
  public static final int MULTI_STATUS = 207;

  public static final String P_METRICS = "metrics";
  public static final String P_ERRORS = "errors";
  private int statusCode;
  private JsonObject result = new JsonObject();
  private final List<ErrorReport> errors = new ArrayList<>();

  private UpdateMetrics metrics;
  public InventoryUpdateOutcome() {

  }

  public InventoryUpdateOutcome(ErrorReport error) {
    this.statusCode = error.statusCode;
    errors.add(error);
  }

  public InventoryUpdateOutcome (JsonObject result) {
    this.result = result;
    if (result.containsKey(P_ERRORS) && result.getValue(P_ERRORS) instanceof JsonArray) {

      for (Object o : result.getJsonArray(P_ERRORS)) {
        errors.add(ErrorReport.makeErrorReportFromJsonString(((JsonObject) o).encode()));
      }
    }
    if (result.containsKey(P_METRICS) && result.getValue(P_METRICS) instanceof JsonObject) {
      metrics = UpdateMetrics.makeMetricsFromJson(result.getJsonObject(P_METRICS));
    }
  }

  public InventoryUpdateOutcome setMetrics (UpdateMetrics metrics) {
    this.metrics = metrics;
    getJson().put(P_METRICS,metrics.asJson());
    return this;
  }

  public boolean hasMetrics () {
    return metrics != null;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public UpdateMetrics getMetrics() {
    return metrics;
  }


  public boolean hasErrors () {
    return ! errors.isEmpty();
  }

  public InventoryUpdateOutcome setErrors (JsonArray errors) {
    getJson().put(P_ERRORS, errors);
    return this;
  }

  public boolean hasError () {
    return errors.size() == 1;
  }

  public ErrorReport getError () {
    if (hasErrors()) {
      return errors.getFirst();
    } else {
      return null;
    }
  }

  public InventoryUpdateOutcome setResponseStatusCode (int status) {
    statusCode = status;
    return this;
  }

  public JsonObject getJson() {
    return result;
  }

  public boolean isError () {
    return hasError();
  }

  public ErrorReport getErrorResponse() {
    if (isError()) {
      return errors.getFirst();
    } else {
      return null;
    }
  }

  public boolean failed () {
    return isError();
  }

  public void respond (RoutingContext routingContext) {
    if (statusCode == OK || statusCode == MULTI_STATUS) {
      responseJson(routingContext, statusCode).end(getJson().encodePrettily());
    } else {
      getErrorResponse().respond(routingContext);
    }
  }
}
