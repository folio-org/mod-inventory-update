package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.test.fakestorage.entitites.InventoryRecord;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public abstract class RecordStorage {
    public final static String TOTAL_RECORDS = "totalRecords";
    // Property keys, JSON responses
    public final static String INSTANCES = "instances";
    public final static String INSTANCE_SETS = "instanceSets";
    public static final String HOLDINGS_RECORDS = "holdingsRecords";
    public static final String ITEMS = "items";
    public static final String INSTANCE_RELATIONSHIPS = "instanceRelationships";
    public static final String PRECEDING_SUCCEEDING_TITLES = "precedingSucceedingTitles";
    public static final String LOCATIONS = "locations";

    public final String STORAGE_NAME = getClass().getSimpleName();
    public boolean failOnDelete = false;
    public boolean failOnCreate = false;
    public boolean failOnUpdate = false;
    public boolean failOnGetRecordById = false;
    public boolean failOnGetRecords = false;
    List<ForeignKey> dependentEntities = new ArrayList<>();
    List<ForeignKey> masterEntities = new ArrayList<>();
    List<String> mandatoryProperties = new ArrayList<>();

    protected FakeInventoryStorage fakeStorage;

    protected final Map<String, InventoryRecord> records = new HashMap<>();
    protected final Logger logger = LoggerFactory.getLogger("fake-inventory-storage");

    public void attachToFakeStorage(FakeInventoryStorage fakeStorage) {
        this.fakeStorage = fakeStorage;
        declareDependencies();
        declareMandatoryProperties();
    }

    // PROPERTY NAME OF THE OBJECT THAT API RESULTS ARE RETURNED IN, IMPLEMENTED PER STORAGE ENTITY
    protected abstract String getResultSetName();

    // INTERNAL DATABASE OPERATIONS - insert() IS DECLARED PUBLIC SO THE TEST SUITE CAN INITIALIZE DATA OUTSIDE THE API.
    public StorageResponse insert (InventoryRecord record) {
        Resp validation = validateCreate(record);

        if (validation.statusCode == 201) {
            record.setFirstVersion();
            records.put(record.getId(), record);
        }
        return new StorageResponse(validation.statusCode, validation.message);
    }

    public static class Resp  {
        public int statusCode;
        public String message;
        public Resp(int status, String message) {
            statusCode = status;
            this.message = message;
        }
    }

    public Resp validateCreate(InventoryRecord record) {
        if (failOnCreate) {
            return new Resp(500, "forced fail");
        }
        if (!record.hasId()) {
            record.generateId();
        }
        if (records.containsKey(record.getId())) {
            logger.error("Fake record storage already contains a record with id " + record.getId() + ", cannot create " + record.getJson().encodePrettily());
            return new Resp(400, "add duplicate key message here");
        }
        logger.debug("Checking foreign keys");
        logger.debug("Got " + masterEntities.size() + " foreign keys");
        for (ForeignKey fk : masterEntities) {
            if (! record.getJson().containsKey(fk.getDependentPropertyName())) {
                logger.error("Foreign key violation, record must contain " + fk.getDependentPropertyName());
                return new Resp(422, "{\"errors\":[{\"message\":\"must not be null\",\"type\":\"1\",\"code\":\"-1\",\"parameters\":[{\"key\":\""+fk.getDependentPropertyName()+"\",\"value\":\"null\"}]}]}");
            }
            if (!fk.getMasterStorage().hasId(record.getJson().getString(fk.getDependentPropertyName()))) {
                logger.error("Foreign key violation " + fk.getDependentPropertyName() + " not found in "+ fk.getMasterStorage().getResultSetName() + ", cannot create " + record.getJson().encodePrettily());
                logger.error(new JsonObject().encode());
                return new Resp (500, new JsonObject("{ \"message\": \"insert or update on table \\\"storage_table\\\" violates foreign key constraint \\\"fkey\\\"\", \"severity\": \"ERROR\", \"code\": \"23503\", \"detail\": \"Key (property value)=(the id) is not present in table \\\"a_referenced_table\\\".\", \"file\": \"ri_triggers.c\", \"line\": \"3266\", \"routine\": \"ri_ReportViolation\", \"schema\": \"diku_mod_inventory_storage\", \"table\": \"storage_table\", \"constraint\": \"a_fkey\" }").encodePrettily());
            } else {
                logger.debug("Found " + record.getJson().getString(fk.getDependentPropertyName()) + " in " + fk.getMasterStorage().getResultSetName());
            }
        }
        for (String mandatory : mandatoryProperties) {
            if (!record.getJson().containsKey(mandatory)) {
                return new Resp(422, new JsonObject("{\"message\" : {\n" + "      \"errors\" : [ {\n" + "        \"message\" : \"must not be null\",\n" + "        \"type\" : \"1\",\n" + "        \"code\" : \"javax.validation.constraints.NotNull.message\",\n" + "        \"parameters\" : [ {\n" + "          \"key\" : \"" + mandatory +"\",\n" + "          \"value\" : \"null\"\n" + "        } ]\n" + "      } ]\n" + "    }}").encodePrettily());
            }
        }
        return new Resp(201,"created");
    }

    protected int update (String id, InventoryRecord record) {

        Resp validation = validateUpdate(id, record);
        if (validation.statusCode == 204) {
            records.put(id, record);
        }
        return validation.statusCode;
    }

    public Resp validateUpdate (String id, InventoryRecord record) {
        if (failOnUpdate) {
            return new Resp(500, "forced faile on update");
        }
        if (record.hasId() && !id.equals(record.getId())) {
            return new Resp(400, "Fake record storage received request to update a record at an ID that doesn't match the ID in the record");
        }
        if (! records.containsKey(id)) {
            return new Resp(404,"Record not found, cannot update " + record.getJson().encodePrettily());
        }
        for (ForeignKey fk : masterEntities) {
            if (! record.getJson().containsKey(fk.getDependentPropertyName())) {
                return new Resp(422, "Foreign key violation, record must contain " + fk.getDependentPropertyName());
            }
            if (!fk.getMasterStorage().hasId(record.getJson().getString(fk.getDependentPropertyName()))) {
                return new Resp(500, "Not found: "+ record.getJson().getString(fk.getDependentPropertyName()) + " in " + fk.getMasterStorage().getResultSetName());
            }
        }
        return new Resp(204,"");
    }

    protected int delete (String id) {
        if (failOnDelete) return 500;
        if (!records.containsKey(id)) {
            logger.error("Record " + id + " not found, cannot delete");
            return 404;
        }
        logger.debug("Dependent entities: " + dependentEntities.size());
        for (ForeignKey fk : dependentEntities) {
            logger.debug("Deleting. Checking dependent " + fk.getDependentStorage().getResultSetName());
            logger.debug("Looking at property " + fk.getDependentPropertyName());
            if (fk.getDependentStorage().hasValue(fk.getDependentPropertyName(), id)) {
               logger.error("Foreign key violation " + records.get(id).getJson().toString() + " has a dependent record in " + fk.getDependentStorage().getResultSetName());
               return 400;
            }
        }
        records.remove(id);
        return 200;
    }

    protected Collection<InventoryRecord> getRecords () {
        return records.values();
    }

    private InventoryRecord getRecord (String id) {
        if (failOnGetRecordById) {
            return null;
        } else {
            InventoryRecord record = records.get( id );
            if (record != null) {
              record.setVersion(record.getVersion() + 1);
            }
            return record;
        }
    }

    // FOREIGN KEY HANDLING
    protected boolean hasId (String id) {
        return records.containsKey(id);
    }

    /**
     * Checks if this storage has a record where this property (presumably a foreign key property) has this value
     * @param fkPropertyName
     * @param value
     * @return
     */
    protected boolean hasValue (String fkPropertyName, String value) {
        for (InventoryRecord record : records.values()) {
            logger.debug("Checking " + record.getJson().toString() + " for value " + value);
            if (record.getJson().containsKey(fkPropertyName) && record.getJson().getString(fkPropertyName).equals(value)) {
                return true;
            }
        }
        return false;
    }

       // USED BY A DEPENDENT ENTITY TO SET UP ITS FOREIGN KEYS BY CALLS to acceptDependant()
    protected abstract void declareDependencies();

       // METHOD ON THE PRIMARY KEY ENTITY TO REGISTER DEPENDENT ENTITIES
    protected void acceptDependant(RecordStorage dependentEntity, String dependentPropertyName) {
        ForeignKey fk = new ForeignKey(dependentEntity, dependentPropertyName, this);
        dependentEntities.add(fk);
        dependentEntity.setMasterEntity(fk);
    }

    protected void setMasterEntity (ForeignKey fk) {
        masterEntities.add(fk);
    }

    protected abstract void declareMandatoryProperties ();

    // API REQUEST HANDLERS

    /**
     * Handles GET request with query parameter
     *
     */
    public void getRecords(RoutingContext routingContext) {
        final String optionalQuery = routingContext.request().getParam("query") != null ?
                decode(routingContext.request().getParam("query")) : null;
        JsonObject responseJson = buildJsonRecordsResponse(optionalQuery);
        if (responseJson != null) {
            respond(routingContext, buildJsonRecordsResponse(optionalQuery), 200);
        } else {
            respondWithMessage(routingContext, (failOnGetRecords ? "Forced " : "") + " Error on getting records", 500);
        }
    }

    /**
     * Handles GET by ID
     */
    protected void getRecordById(RoutingContext routingContext) {
        final String id = routingContext.pathParam("id");
        InventoryRecord record = getRecord(id);

        if (record != null) {
            respond(routingContext, record.getJson(), 200);
        } else {
            respondWithMessage(routingContext, (failOnGetRecordById ? "Forced error on get from " : "No record with ID " + id + " in ") + STORAGE_NAME, 404);
        }
    }

    /**
     * Handles DELETE
     */
    protected void deleteRecord (RoutingContext routingContext) {
        final String id = routingContext.pathParam("id");
        int code = delete(id);

        if (code == 200) {
            respond(routingContext, new JsonObject(), code);
        } else {
            respondWithMessage(routingContext, (failOnDelete ? "Forced " : "") + "Error deleting from " + STORAGE_NAME, code);
        }
    }

    /**
     * Handles DELETE ALL
     */
    protected void deleteAll (RoutingContext routingContext) {
        records.clear();
        respond(routingContext, new JsonObject("{\"message\": \"all records deleted\"}"), 200);
    }


    /**
     * Handles POST
     *
     */
    protected void createRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        StorageResponse response = insert(new InventoryRecord(recordJson));
        if (response.statusCode == 201) {
            respond(routingContext, recordJson, response.statusCode);
        } else {
            respondWithMessage(routingContext, response.responseBody, response.statusCode);
        }
    }


    /**
     * Handles PUT
     *
     */
    protected void updateRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.getBodyAsString());
        String id = routingContext.pathParam("id");
        int code = update(id, new InventoryRecord(recordJson));
        if (code == 204) {
            respond(routingContext, code);
        } else {
            respondWithMessage(routingContext, (failOnUpdate ? "Forced " : "") + "Error updating record in " + STORAGE_NAME, code);
        }
    }

    protected void upsertRecords (RoutingContext routingContext) {
        UUID transaction = UUID.randomUUID();
        JsonObject requestJson = new JsonObject(routingContext.getBodyAsString());
        JsonArray recordsJson = requestJson.getJsonArray(getResultSetName());
        for (Object o : recordsJson) {
            InventoryRecord record = new InventoryRecord((JsonObject) o);
            if (hasId(record.getId())) {
                Resp validation = validateUpdate(record.getId(), record);
                if (validation.statusCode != 204 ) {
                    respondWithMessage(routingContext, validation.message, validation.statusCode);
                    return;
                }
            } else {
                Resp validation = validateCreate(record);
                if (validation.statusCode != 201 ) {
                    respondWithMessage(routingContext, validation.message, validation.statusCode);
                    return;
                }
            }
        }
        for (Object o : recordsJson) {
            InventoryRecord record = new InventoryRecord((JsonObject) o);
            if (hasId(record.getId())) {
                update(record.getId(), record);
            } else {
                insert(record);
            }
        }
        respond(routingContext, requestJson, 201);
    }
    // HELPERS FOR RESPONSE PROCESSING

    JsonObject buildJsonRecordsResponse(String optionalQuery) {
        if (failOnGetRecords) return null;
        JsonObject response = new JsonObject();
        JsonArray jsonRecords = new JsonArray();
        getRecords().forEach( record -> {
            if (optionalQuery == null || record.match(optionalQuery)) {
                jsonRecords.add(record.getJson());
            }});
        response.put(getResultSetName(), jsonRecords);
        response.put(TOTAL_RECORDS, jsonRecords.size());
        return response;
    }

    /**
     * Respond with JSON and status code
     *
     * @param responseJson the response
     * @param code the status code
     */
    protected static void respond(RoutingContext routingContext, JsonObject responseJson, int code) {
        routingContext.response().headers().add("Content-Type", "application/json");
        routingContext.response().setStatusCode(code);
        routingContext.response().end(responseJson.encodePrettily());
    }

    /**
     * Respond with status code
     *
     * @param code the status code
     */
    protected static void respond(RoutingContext routingContext, int code) {
        routingContext.response().headers().add("Content-Type", "application/json");
        routingContext.response().setStatusCode(code);
        routingContext.response().end();
    }

    /**
     * Respond with text message (error response)
     *
     * @param res error condition
     */
    protected static void respondWithMessage(RoutingContext routingContext, Throwable res) {
        routingContext.response().setStatusCode(500);
        routingContext.response().end(res.getMessage());
    }

    protected static void respondWithMessage (RoutingContext routingContext, String message, int code) {
        routingContext.response().setStatusCode(code);
        routingContext.response().end(message);

    }


    // UTILS

    public static String decode (String string) {
      return URLDecoder.decode(string, StandardCharsets.UTF_8);
    }

    public static String encode (String string) {
      return URLEncoder.encode(string, StandardCharsets.UTF_8);
    }

    public void logRecords (Logger logger) {
        records.values().stream().forEach(record -> logger.debug(record.getJson().encodePrettily()));
    }

    public static class ForeignKey {

        private final RecordStorage dependentStorage;
        private final String dependentPropertyName;
        private final RecordStorage masterStorage;

        public ForeignKey (RecordStorage dependentStorage, String dependentPropertyName, RecordStorage masterStorage) {
            this.dependentStorage = dependentStorage;
            this.dependentPropertyName = dependentPropertyName;
            this.masterStorage = masterStorage;
        }

        public RecordStorage getDependentStorage() {
            return dependentStorage;
        }

        public String getDependentPropertyName() {
            return dependentPropertyName;
        }

        public RecordStorage getMasterStorage() {
            return masterStorage;
        }

    }
}
