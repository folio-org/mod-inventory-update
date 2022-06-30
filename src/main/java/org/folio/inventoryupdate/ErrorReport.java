package org.folio.inventoryupdate;

import io.vertx.core.json.DecodeException;
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


  private static final String CATEGORY = "category";
  private static final String TRANSACTION = "transaction";
  private static final String ENTITY_TYPE = "entityType";
  private static final String STATUS_CODE = "statusCode";
  private static final String MESSAGE = "message";
  private static final String SHORT_MESSAGE = "shortMessage";
  private static final String ENTITY = "entity";
  private static final String DETAILS = "details";


  ErrorCategory category;

  InventoryRecord.Entity entityType;
  String transaction;
  int statusCode;
  String shortMessage;
  String messageAsString;
  JsonObject messageAsJson;
  JsonObject entity;
  JsonObject details;



  public ErrorReport(ErrorCategory category, int statusCode, String message) {
    this.category = category;
    this.messageAsString = message;
    if (message != null) {
      this.shortMessage = message.substring(0, Math.min(message.length(),40));
    } else {
      this.shortMessage = "";
    }
    this.statusCode = statusCode;
  }

  public ErrorReport(ErrorCategory category, int statusCode, Object message) {
    this.category = category;
    if (message instanceof JsonObject) {
      this.messageAsJson = (JsonObject) message;
      this.shortMessage = "";
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
    return new ErrorReport(
            getCategoryFromString(json.getString(CATEGORY)),
            json.getInteger(STATUS_CODE),
            json.getValue(MESSAGE))
            .setEntity(json.getJsonObject(ENTITY))
            .setTransaction(json.getString(TRANSACTION))
            .setEntityType(getEntityTypeFromString(json.getString(ENTITY_TYPE)));
  }

  public static boolean isAnErrorReportJson (String maybeJson) {
    if (isJsonString(maybeJson)) {
      JsonObject maybeErrorReportJson = new JsonObject(maybeJson);
      return maybeErrorReportJson.containsKey(CATEGORY) &&
              getCategoryFromString(maybeErrorReportJson.getString(CATEGORY)) != null &&
              maybeErrorReportJson.containsKey(STATUS_CODE) &&
              maybeErrorReportJson.containsKey(MESSAGE);
    } else {
      return false;
    }
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


  private static boolean isJsonString (String maybeJson) {
    try {
      new JsonObject(maybeJson);
      return true;
    } catch (DecodeException de) {
      return false;
    }
  }


  public InventoryRecord.Entity getEntityType () {
    return entityType;
  }

  public ErrorReport setEntityType(InventoryRecord.Entity entityType) {
    this.entityType = entityType;
    return this;
  }

  public String getTransaction() {
    return transaction;
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

  public ErrorReport setMessageAsString(String messageAsString) {
    this.messageAsString = messageAsString;
    this.messageAsJson = null;
    return this;
  }

  public ErrorReport setMessageAsJson (JsonObject messageAsJson) {
    this.messageAsJson = messageAsJson;
    this.messageAsString = null;
    return this;
  }

  public JsonObject getMessageAsJson () {
    return messageAsJson;
  }

  public JsonObject getEntity() {
    return entity;
  }

  public ErrorReport setEntity(JsonObject entity) {
    this.entity = entity;
    return this;
  }

  public JsonObject getDetails() {
    return details;
  }

  public ErrorReport setDetails(JsonObject details) {
    this.details = details;
    return this;
  }

  public JsonObject asJson () {
    JsonObject errorJson = new JsonObject();
    errorJson.put(CATEGORY, category.toString());
    if (messageAsJson != null && !messageAsJson.isEmpty()) {
      errorJson.put(MESSAGE, messageAsJson);
    } else {
      errorJson.put(MESSAGE, messageAsString);
    }
    errorJson.put(SHORT_MESSAGE, shortMessage);
    errorJson.put(ENTITY_TYPE, (entityType == null ? "" : entityType.toString()));
    errorJson.put(ENTITY, entity == null ? new JsonObject() : entity);
    errorJson.put(STATUS_CODE, statusCode);
    errorJson.put(DETAILS, details == null ? new JsonObject() : details);
    return errorJson;
  }

  public String asJsonString () {
    return asJson().encode();
  }

  public String asJsonPrettily () {
    return asJson().encodePrettily();
  }

  public void respond(RoutingContext routingContext) {
    responseJson(routingContext, getStatusCode()).end(asJsonString());
  }
}