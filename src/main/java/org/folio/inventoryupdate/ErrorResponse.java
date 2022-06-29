package org.folio.inventoryupdate;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.entities.InventoryRecord;

import static org.folio.okapi.common.HttpResponse.responseJson;

public class ErrorResponse {

  public enum ErrorCategory {
    VALIDATION,
    STORAGE,
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



  public ErrorResponse(ErrorCategory category, int statusCode, String message) {
    this.category = category;
    this.messageAsString = message;
    if (message != null) {
      this.shortMessage = message.substring(0, Math.min(message.length()-1,40));
    } else {
      this.shortMessage = "";
    }
    this.statusCode = statusCode;
  }

  public ErrorResponse(ErrorCategory category, int statusCode, JsonObject message) {
    this.category = category;
    this.messageAsJson = message;
    this.shortMessage = "";
    this.statusCode = statusCode;
  }

  public String getEntityTypeAsString() {
    if (entityType != null) {
      return entityType.toString();
    } else {
      return null;
    }
  }

  public InventoryRecord.Entity getEntityType () {
    return entityType;
  }

  public ErrorResponse setEntityType(InventoryRecord.Entity entityType) {
    this.entityType = entityType;
    return this;
  }

  public String getTransaction() {
    return transaction;
  }

  public ErrorResponse setTransaction(String transaction) {
    this.transaction = transaction;
    return this;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public ErrorResponse setStatusCode(int statusCode) {
    this.statusCode = statusCode;
    return this;
  }

  public String getShortMessage() {
    return shortMessage;
  }

  public ErrorResponse setShortMessage(String shortMessage) {
    this.shortMessage = shortMessage;
    return this;
  }

  public String getMessageAsString() {
    return messageAsString;
  }

  public ErrorResponse setMessageAsString(String messageAsString) {
    this.messageAsString = messageAsString;
    this.messageAsJson = null;
    return this;
  }

  public ErrorResponse setMessageAsJson (JsonObject messageAsJson) {
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

  public ErrorResponse setEntity(JsonObject entity) {
    this.entity = entity;
    return this;
  }

  public JsonObject getDetails() {
    return details;
  }

  public ErrorResponse setDetails(JsonObject details) {
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
