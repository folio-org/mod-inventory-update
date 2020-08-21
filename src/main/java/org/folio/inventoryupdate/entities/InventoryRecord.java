package org.folio.inventoryupdate.entities;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

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
        HOLDINGSRECORD,
        ITEM,
        LOCATION
    }

    protected JsonObject jsonRecord;
    protected JsonObject error = new JsonObject();
    protected Entity type;
    protected Transaction transaction = Transaction.UNKNOWN;
    protected Outcome outcome = Outcome.PENDING;

    public void setTransition (Transaction transaction) {
        this.transaction = transaction;
    }

    public Transaction getTransaction () {
        return transaction;
    }

    public boolean isDeleting () {
        return (transaction == Transaction.DELETE);
    }

    public boolean isUpdating () {
        return (transaction == Transaction.UPDATE);
    }

    public boolean isCreating () {
        return (transaction == Transaction.CREATE);
    }

    public boolean stateUnknown () {
        return (transaction == Transaction.UNKNOWN);
    }

    public String generateUUID () {
        UUID uuid = UUID.randomUUID();
        jsonRecord.put("id", uuid.toString());
        return uuid.toString();
    }

    public void setUUID (String uuid) {
        jsonRecord.put("id", uuid);
    }

    public String UUID () {
        return jsonRecord.getString("id");
    }

    public boolean hasUUID () {
        return (jsonRecord.getString("id") != null);
    }

    public String getHRID () {
        return jsonRecord.getString("hrid");
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
        return type;
    }

    public void setOutcome (Outcome outcome) {
        this.outcome = outcome;
    }

    public Outcome getOutcome () {
        return this.outcome;
    }

    public void complete() {
        this.outcome = Outcome.COMPLETED;
    }

    public boolean completed() {
        return this.outcome == Outcome.COMPLETED;
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

    public void logError (String error, int statusCode) {
        Object message = maybeJson(error);
        logError(error, statusCode, findShortMessage(message));
    }

    public void logError (String error, int statusCode, String shortMessage) {
        this.error.put("entityType", entityType());
        this.error.put("transaction", getTransaction());
        this.error.put("statusCode", statusCode);
        this.error.put("shortMessage", shortMessage);
        this.error.put("message", maybeJson(error));
        this.error.put("entity", jsonRecord);
    }

    protected static Object maybeJson (String message) {
        try {
          return new JsonObject(message);
        } catch (DecodeException de) {
            if (message.startsWith("ErrorMessage") && message.contains("SQLSTATE")) {
                // looks like PostgreSQL error, try to parse to JSON
                JsonObject postgreSQLError = parsePostgreSQLErrorTupples(message);
                if (!postgreSQLError.isEmpty()) {
                    return postgreSQLError;
                }
            }
            return message;
        }
    }

    protected String findShortMessage (Object inventoryMessage) {
        String shortMessage = "";
        if (inventoryMessage instanceof JsonObject) {
            JsonObject jsonFormattedError = (JsonObject)inventoryMessage;
            if (jsonFormattedError.containsKey("errors") && jsonFormattedError.getValue("errors") instanceof JsonArray) {
                // Looks like FOLIO json schema validation error
                shortMessage = getMessageFromFolioSchemaValidationError(shortMessage, jsonFormattedError);
            } else if (jsonFormattedError.containsKey("Message")) {
                // Name of the essential message property in raw PostgreSQL error messages
                shortMessage = jsonFormattedError.getString("Message");
            } else {
                // fallback
                shortMessage = "Error: " + getTransaction() + " of " + entityType();
            }
        } else if (inventoryMessage instanceof String && inventoryMessage.toString().length()>1) {
            // In some error scenarios, Inventory just returns a simple string.
            shortMessage = inventoryMessage.toString().substring(0, Math.min(inventoryMessage.toString().length()-1,60));
        } else {
            // fallback
            shortMessage = "Error: " + getTransaction() + " of " + entityType();
        }
        return shortMessage;
    }

    private String getMessageFromFolioSchemaValidationError(String shortMessage, JsonObject jsonFormattedError) {
        JsonArray errors = jsonFormattedError.getJsonArray("errors");
        if (errors.size()>0 && errors.getValue(0) instanceof JsonObject) {
            JsonObject firstError = errors.getJsonObject(0);
            if (firstError.containsKey("message") && firstError.getValue("message") instanceof String) {
                shortMessage += firstError.getString("message");
            }
            if (firstError.containsKey("parameters") && firstError.getValue("parameters") instanceof JsonArray) {
                JsonArray parameters = firstError.getJsonArray("parameters");
                if (parameters.size()>0 && parameters.getValue(0) instanceof JsonObject) {
                    JsonObject firstParameter = parameters.getJsonObject(0);
                    shortMessage +=  ": " + firstParameter.getValue("key");
                }
            }
        }
        return shortMessage;
    }


    // Sample:  "ErrorMessage(fields=[(Severity, ERROR), (V, ERROR), (SQLSTATE, 23503),
    //           (Message, insert or update on table \"holdings_record\" violates foreign key constraint \"holdings_record_permanentlocationid_fkey\"),
    //           (Detail, Key (permanentlocationid)=(53cf956f-c1df-410b-8bea-27f712cca7c9) is not present in table \"location\".),
    //           (s, diku_mod_inventory_storage), (t, holdings_record), (n, holdings_record_permanentlocationid_fkey),
    //           (File, ri_triggers.c), (Line, 3266), (Routine, ri_ReportViolation)])"

    // everything between the square brackets of:   ErrorMessage(fields=[(),(),()])
    private static Pattern POSTGRESQL_ERROR_TUPPLE_ARRAY_PATTERN = Pattern.compile("(?<=\\[).+?(?=\\])");
    // capture tupples, enclosed in round brackets:  (0),(1),(2)
    private static Pattern POSTGRESQL_ERROR_TUPPLE_GROUPS_PATTERN = Pattern.compile("[^,(]*(?:\\([^)]*\\))*[^,]*");

    private static JsonObject parsePostgreSQLErrorTupples (String message) {

        final Matcher arrayMatcher = POSTGRESQL_ERROR_TUPPLE_ARRAY_PATTERN.matcher(message);
        JsonObject messageJson = new JsonObject();
        if (arrayMatcher.find()) {
            String arrayString = arrayMatcher.group(0);
            System.out.println(arrayString);
            Matcher tupplesMatcher = POSTGRESQL_ERROR_TUPPLE_GROUPS_PATTERN.matcher(arrayString);
            while (tupplesMatcher.find()) {
                String tuppleString = tupplesMatcher.group(0).trim();
                // trim the round brackets
                tuppleString = tuppleString.replaceFirst("\\(", "");
                tuppleString = tuppleString.replaceAll("\\)+$", "");
                String[] keyval = tuppleString.split(", ");
                if (keyval.length==2) {
                   messageJson.put(keyval[0],keyval[1]);
                }
            }
        }
        return messageJson;
    }

    public JsonObject getError () {
        return error;
    }

    public abstract void skipDependants ();

}