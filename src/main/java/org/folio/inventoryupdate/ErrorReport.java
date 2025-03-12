package org.folio.inventoryupdate;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.InventoryRecord;

import static org.folio.inventoryupdate.entities.InventoryRecord.getEntityTypeFromString;
import static org.folio.okapi.common.HttpResponse.responseJson;

public class ErrorReport {

  public enum ErrorCategory {
    VALIDATION,
    STORAGE,
    BATCH_STORAGE,
    INTERNAL
  }

  public static final int BAD_REQUEST = 400;
  public static final int NOT_FOUND = 404;
  public static final int UNPROCESSABLE_ENTITY = 422;
  public static final int INTERNAL_SERVER_ERROR = 500;


  private static final String P_CATEGORY = "category";
  private static final String P_TRANSACTION = "transaction";
  private static final String P_ENTITY_TYPE = "entityType";
  private static final String P_STATUS_CODE = "statusCode";
  private static final String P_MESSAGE = "message";
  private static final String P_SHORT_MESSAGE = "shortMessage";
  private static final String P_ENTITY = "entity";
  private static final String P_DETAILS = "details";
  private static final String P_REQUEST_JSON = "requestJson";


  ErrorCategory category;

  InventoryRecord.Entity entityType;
  String transaction;
  int statusCode;
  String shortMessage;
  String messageAsString;
  JsonObject messageAsJson;
  JsonObject entity;
  JsonObject requestJson;
  JsonObject details = new JsonObject();

  public ErrorReport(ErrorCategory category, int statusCode, Object message) {
    this.category = category;
    if (message instanceof JsonObject) {
      this.messageAsJson = (JsonObject) message;
      if (messageAsJson.containsKey(P_SHORT_MESSAGE)) {
        this.shortMessage = messageAsJson.getString(P_SHORT_MESSAGE);
      } else {
        this.shortMessage = "";
      }
    } else {
      if (message != null) {
        this.messageAsString = message.toString();
        this.shortMessage = messageAsString.substring(0, Math.min(messageAsString.length(), 40));
      } else {
        messageAsString = "";
        this.shortMessage = "";
      }
    }
    this.statusCode = statusCode;
  }

  public static ErrorReport makeErrorReportFromJsonString(String jsonString) {
      JsonObject json = new JsonObject(jsonString);
      return new ErrorReport(getCategoryFromString(json.getString(P_CATEGORY)),
              json.getInteger(P_STATUS_CODE),
              json.getValue(P_MESSAGE))
              .setShortMessage(json.getString(P_SHORT_MESSAGE))
              .setEntity(json.getJsonObject(P_ENTITY))
              .setTransaction(json.getString(P_TRANSACTION))
              .setEntityType(getEntityTypeFromString(json.getString(P_ENTITY_TYPE)))
              .setRequestJson(json.getJsonObject(P_REQUEST_JSON))
              .setDetails(json.getJsonObject(P_DETAILS));
  }

  public boolean isBatchStorageError () {
    return category == ErrorCategory.BATCH_STORAGE;
  }

  public static ErrorCategory getCategoryFromString (String errorCategory) {

    switch (errorCategory.toUpperCase()) {
      case "STORAGE":
        return ErrorCategory.STORAGE;
      case "BATCH_STORAGE":
        return ErrorCategory.BATCH_STORAGE;
      case "VALIDATION":
        return ErrorCategory.VALIDATION;
      case "INTERNAL":
        return ErrorCategory.INTERNAL;
      default:
        return null;
    }
  }

  public InventoryRecord.Entity getEntityType () {
    return entityType;
  }

  public ErrorReport setEntityType(InventoryRecord.Entity entityType) {
    this.entityType = entityType;
    return this;
  }

  public ErrorReport setRequestJson(JsonObject requestJson) {
    this.requestJson = requestJson;
    return this;
  }

  public JsonObject getRequestJson() {
    return this.requestJson;
  }

  public ErrorReport setTransaction(String transaction) {
    this.transaction = transaction;
    return this;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public ErrorReport setStatusCode(int statusCode) {
    this.statusCode = statusCode;
    return this;
  }

  public String getShortMessage() {
    return shortMessage;
  }

  public ErrorReport setShortMessage(String shortMessage) {
    this.shortMessage = shortMessage;
    return this;
  }

  public String getMessageAsString() {
    return messageAsString;
  }

  public JsonObject getEntity() {
    return entity;
  }

  public ErrorReport setEntity(JsonObject entity) {
    this.entity = entity;
    return this;
  }

  public ErrorReport setDetails(JsonObject details) {
    this.details = details;
    return this;
  }

  public ErrorReport addDetail (String key, String value) {
    if (details.containsKey(key)) {
      if (details.getValue(key) instanceof JsonArray) {
        JsonArray array = new JsonArray();
        array.add(value).addAll(details.getJsonArray(key));
        details.put(key, array);
      }
    } else {
      JsonArray array = new JsonArray();
      array.add(value);
      details.put(key, array);
    }
    return this;
  }

  public JsonObject asJson () {
    JsonObject errorJson = new JsonObject();
    errorJson.put(P_CATEGORY, category.toString());
    if (messageAsJson != null && !messageAsJson.isEmpty()) {
      errorJson.put(P_MESSAGE, messageAsJson);
    } else {
      errorJson.put(P_MESSAGE, messageAsString);
    }
    errorJson.put(P_SHORT_MESSAGE, shortMessage);
    errorJson.put(P_ENTITY_TYPE, (entityType == null ? "" : entityType.toString()));
    errorJson.put(P_ENTITY, entity == null ? new JsonObject() : entity);
    errorJson.put(P_STATUS_CODE, statusCode);
    errorJson.put(P_REQUEST_JSON, requestJson);
    errorJson.put(P_DETAILS, details == null ? new JsonObject() : details);
    return errorJson;
  }

  public String asJsonString () {
    return asJson().encode();
  }

  public String asJsonPrettily () {
    return asJson().encodePrettily();
  }

  public void respond(RoutingContext routingContext) {
    var code = getStatusCode();
    if (code < 100 || code > 599) {
      // code might be 0 if not set.
      // nginx, curl, restassured, vert.x (as client), ... handle 0 as internal error and don't report/log the body.
      //
      // https://datatracker.ietf.org/doc/html/rfc9110#section-15
      // "The status code of a response is a three-digit integer code"
      // "Values outside the range 100..599 are invalid. Implementations often use three-digit integer values
      // outside of that range (i.e., 600..999) for internal communication of non-HTTP status (e.g., library errors)."
      code = 500;
    }
    responseJson(routingContext, code).end(asJsonPrettily());
  }
}
