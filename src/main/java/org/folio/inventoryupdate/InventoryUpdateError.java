package org.folio.inventoryupdate;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.InventoryRecord;

public class InventoryUpdateError {

  public enum ErrorCategory {
    VALIDATION,
    STORAGE,
    INTERNAL
  }

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



  public InventoryUpdateError (ErrorCategory category, String message) {
    this.category = category;
    this.messageAsString = message;
    if (message != null) {
      this.shortMessage = message.substring(0, Math.min(message.length()-1,40));
    } else {
      this.shortMessage = "";
    }
  }

  public InventoryUpdateError (ErrorCategory category, JsonObject message) {
    this.category = category;
    this.messageAsJson = message;
    this.shortMessage = "";
  }

  public String getEntityType() {
    if (entityType != null) {
      return entityType.toString();
    } else {
      return null;
    }
  }

  public InventoryUpdateError setEntityType(InventoryRecord.Entity entityType) {
    this.entityType = entityType;
    return this;
  }

  public String getTransaction() {
    return transaction;
  }

  public InventoryUpdateError setTransaction(String transaction) {
    this.transaction = transaction;
    return this;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public InventoryUpdateError setStatusCode(int statusCode) {
    this.statusCode = statusCode;
    return this;
  }

  public String getShortMessage() {
    return shortMessage;
  }

  public InventoryUpdateError setShortMessage(String shortMessage) {
    this.shortMessage = shortMessage;
    return this;
  }

  public String getMessageAsString() {
    return messageAsString;
  }

  public InventoryUpdateError setMessageAsString(String messageAsString) {
    this.messageAsString = messageAsString;
    this.messageAsJson = null;
    return this;
  }

  public InventoryUpdateError setMessageAsJson (JsonObject messageAsJson) {
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

  public InventoryUpdateError setEntity(JsonObject entity) {
    this.entity = entity;
    return this;
  }

  public JsonObject getDetails() {
    return details;
  }

  public InventoryUpdateError setDetails(JsonObject details) {
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
}
