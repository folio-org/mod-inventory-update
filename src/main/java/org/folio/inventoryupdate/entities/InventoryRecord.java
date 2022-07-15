package org.folio.inventoryupdate.entities;

import java.util.UUID;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.ErrorReport;

import static org.folio.inventoryupdate.ErrorReport.UNPROCESSABLE_ENTITY;


/**
 * Base class for Inventory entities (Instances, HoldingsRecords, Items)
 *
 * Contains flags for the transaction (to be) performed for a record of the given entity
 * and the eventual outcome of that transaction and methods for parsing errors in case the
 * outcome is FAILED.
 */
public abstract class InventoryRecord {

    public enum Transaction {
        UNKNOWN,
        CREATE,
        UPDATE,
        DELETE,
        GET,
        NONE
    }

    public enum Outcome {
        PENDING,
        COMPLETED,
        FAILED,
        SKIPPED
    }

    public enum Entity {
        INSTANCE,
        HOLDINGS_RECORD,
        ITEM,
        LOCATION,
        INSTANCE_RELATIONSHIP,
        INSTANCE_TITLE_SUCCESSION
    }

    protected JsonObject jsonRecord;
    protected JsonObject originJson;
    public static final String VERSION = "_version";
    protected ErrorReport error;
    protected Entity entityType;
    protected Transaction transaction = Transaction.UNKNOWN;
    protected Outcome outcome = Outcome.PENDING;
    private static final String MESSAGE = "message";
    private static final String ERRORS = "errors";
    private static final String PARAMETERS = "parameters";

    protected static final Logger logger = LoggerFactory.getLogger("inventory-update");

    public void setTransition (Transaction transaction) {
        this.transaction = transaction;
    }

    public Transaction getTransaction () {
        return transaction;
    }

    public boolean isDeleting () {
        return (transaction == Transaction.DELETE);
    }

    public boolean isCreating () {
        return (transaction == Transaction.CREATE);
    }

    public boolean isUpdating () {
        return (transaction == Transaction.UPDATE);
    }

    public String generateUUID () {
        UUID uuid = UUID.randomUUID();
        jsonRecord.put("id", uuid.toString());
        return uuid.toString();
    }

    public void setUUID (String uuid) {
        jsonRecord.put("id", uuid);
    }

    public String getUUID() {
        return jsonRecord.getString("id");
    }

    public boolean hasUUID () {
        return (jsonRecord.getString("id") != null);
    }

    public String getHRID () {
        return jsonRecord.getString("hrid");
    }

    public Integer getVersion () {
        return jsonRecord.getInteger( VERSION );
    }

    public InventoryRecord setVersion (int version) {
        jsonRecord.put(VERSION, version);
        return this;
    }

    public JsonObject asJson() {
        return jsonRecord;
    }

    public String asJsonString() {
        if (jsonRecord != null) {
            return jsonRecord.toString();
        } else {
            return "{}";
        }
    }

    public Entity entityType () {
        return entityType;
    }

    public Outcome getOutcome () {
        return this.outcome;
    }

    public JsonObject getOriginJson () {
        return this.originJson;
    }

    public void complete() {
        this.outcome = Outcome.COMPLETED;
    }

    public void fail() {
        this.outcome = Outcome.FAILED;
    }

    public boolean failed() {
        return this.outcome == Outcome.FAILED;
    }

    public void skip() {
        this.outcome = Outcome.SKIPPED;
    }

    public boolean skipped() {
        return this.outcome == Outcome.SKIPPED;
    }

    public void logError (String error, int statusCode, ErrorReport.ErrorCategory category, JsonObject originJson) {
        Object message = maybeJson(error);
        logError(error, statusCode, category, findShortMessage(message), originJson);
    }

    public void logError (String error, int statusCode, ErrorReport.ErrorCategory category, String shortMessage, JsonObject originJson) {
        Object message = maybeJson(error);
        this.error = new ErrorReport(
                category,
                UNPROCESSABLE_ENTITY,
                message)
                .setEntityType(entityType())
                .setTransaction(getTransaction() == null ? "" : getTransaction().toString())
                .setStatusCode(statusCode)
                .setShortMessage(shortMessage)
                .setEntity(jsonRecord)
                .setRequestJson(originJson);
    }

    protected static Object maybeJson (String message) {
        try {
          return  new JsonObject(message);
        } catch (DecodeException de) {
            return message;
        }
    }

    protected String findShortMessage (Object inventoryMessage) {
        String shortMessage;
        if (inventoryMessage instanceof JsonObject) {
            JsonObject jsonFormattedError = (JsonObject)inventoryMessage;
            if (jsonFormattedError.containsKey(ERRORS) && jsonFormattedError.getValue(ERRORS) instanceof JsonArray) {
                // Looks like FOLIO json schema validation error
                shortMessage = getMessageFromFolioSchemaValidationError(jsonFormattedError);
            } else if (jsonFormattedError.containsKey(MESSAGE)) {
                // Name of the essential message property in raw PostgreSQL error messages
                shortMessage = jsonFormattedError.getString(MESSAGE);
            } else {
                // fallback
                shortMessage = "Error: " + getTransaction() + " of " + entityType();
            }
        } else if (inventoryMessage instanceof String && inventoryMessage.toString().length()>1) {
            // In some error scenarios, Inventory just returns a simple string.
            shortMessage = inventoryMessage.toString().substring(0, Math.min(inventoryMessage.toString().length(),60));
        } else {
            // fallback
            shortMessage = "Error: " + getTransaction() + " of " + entityType();
        }
        return shortMessage;
    }

    private String getMessageFromFolioSchemaValidationError(JsonObject jsonFormattedError) {
        String shortMessage = "";
        JsonArray errors = jsonFormattedError.getJsonArray(ERRORS);
        if (errors.size()>0 && errors.getValue(0) instanceof JsonObject) {
            JsonObject firstError = errors.getJsonObject(0);
            if (firstError.containsKey(MESSAGE) && firstError.getValue(MESSAGE) instanceof String) {
                shortMessage += firstError.getString(MESSAGE);
            }
            if (firstError.containsKey(PARAMETERS) && firstError.getValue(PARAMETERS) instanceof JsonArray) {
                JsonArray parameters = firstError.getJsonArray(PARAMETERS);
                if (parameters.size()>0 && parameters.getValue(0) instanceof JsonObject) {
                    JsonObject firstParameter = parameters.getJsonObject(0);
                    shortMessage +=  ": " + firstParameter.getValue("key");
                }
            }
        }
        return shortMessage;
    }

    public JsonObject getErrorAsJson() {
        return error.asJson();
    }

    public ErrorReport getErrorReport () {
        return error;
    }

    public abstract void skipDependants ();

    public static InventoryRecord.Entity getEntityTypeFromString (String entityType) {
        switch (entityType.toUpperCase()) {
            case "INSTANCE":
                return InventoryRecord.Entity.INSTANCE;
            case "ITEM":
                return InventoryRecord.Entity.ITEM;
            case "HOLDINGS_RECORD":
                return InventoryRecord.Entity.HOLDINGS_RECORD;
            case "INSTANCE_RELATIONSHIP":
                return InventoryRecord.Entity.INSTANCE_RELATIONSHIP;
            case "INSTANCE_TITLE_SUCCESSION":
                return InventoryRecord.Entity.INSTANCE_TITLE_SUCCESSION;
            case "LOCATION":
                return InventoryRecord.Entity.LOCATION;
            default:
                return null;
        }
    }

}