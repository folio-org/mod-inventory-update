package org.folio.inventoryupdate.importing.test.fakestorage;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public abstract class RecordStorage {
    public static final String TOTAL_RECORDS = "totalRecords";
    public final String storageName = getClass().getSimpleName();
    public boolean failOnDelete = false;
    public boolean failOnCreate = false;
    public boolean failOnUpdate = false;
    public boolean failOnGetRecordById = false;
    public boolean failOnGetRecords = false;
    final List<ForeignKey> dependentEntities = new ArrayList<>();
    final List<ForeignKey> masterEntities = new ArrayList<>();
    final List<String> mandatoryProperties = new ArrayList<>();
    final List<String> uniqueProperties = new ArrayList<>();

    protected FakeFolioApis fakeStorage;

    protected final Map<String, FolioApiRecord> records = new HashMap<>();

    public static final Logger logger = LogManager.getLogger("fake-folio-storage");

    public void attachToFakeStorage(FakeFolioApis fakeStorage) {
        this.fakeStorage = fakeStorage;
        declareDependencies();
        declareMandatoryProperties();
        declareUniqueProperties();
    }

    // PROPERTY NAME OF THE OBJECT THAT API RESULTS ARE RETURNED IN, IMPLEMENTED PER STORAGE ENTITY
    protected abstract String getResultSetName();

    // INTERNAL DATABASE OPERATIONS - insert() IS DECLARED PUBLIC SO THE TEST SUITE CAN INITIALIZE DATA OUTSIDE THE API.
    public StorageResponse insert (FolioApiRecord folioApiRecord) {
        Resp validation = validateCreate(folioApiRecord);

        if (validation.statusCode == 201) {
            folioApiRecord.setFirstVersion();
            records.put(folioApiRecord.getId(), folioApiRecord);
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

    public Resp validateCreate(FolioApiRecord folioApiRecord) {
        if (failOnCreate) {
            return new Resp(500, "forced fail");
        }
        if (!folioApiRecord.hasId()) {
            folioApiRecord.generateId();
        }
        if (records.containsKey(folioApiRecord.getId())) {
            logger.error("Fake record storage already contains a record with id {}, cannot create {}",
                folioApiRecord.getId(), folioApiRecord.getJson().encodePrettily());
            return new Resp(400, "Record storage already contains a record with id " + folioApiRecord.getId());
        }
        for (FolioApiRecord existingRecord : records.values()) {
            for (String nameOfUniqueProperty : uniqueProperties) {
                if (folioApiRecord.getStringValue(nameOfUniqueProperty) != null
                    && existingRecord.getStringValue(nameOfUniqueProperty) != null
                    && folioApiRecord.getStringValue(nameOfUniqueProperty).equals(existingRecord.getStringValue(nameOfUniqueProperty))) {
                        return new Resp(400,  this.storageName +" already contains a record with " + nameOfUniqueProperty + " = " + folioApiRecord.getStringValue(nameOfUniqueProperty));
                }
            }
        }
        logger.debug("Checking foreign keys");
        logger.debug("Got {} foreign keys", masterEntities.size());
        for (ForeignKey fk : masterEntities) {
            if (! folioApiRecord.getJson().containsKey(fk.getDependentPropertyName())) {
                logger.error("Foreign key violation, record must contain {}", fk.getDependentPropertyName());
                return new Resp(422, "{\"errors\":[{\"message\":\"must not be null\",\"type\":\"1\",\"code\":\"-1\",\"parameters\":[{\"key\":\""+fk.getDependentPropertyName()+"\",\"value\":\"null\"}]}]}");
            }
            if (!fk.getMasterStorage().hasId(folioApiRecord.getJson().getString(fk.getDependentPropertyName()))) {
                logger.error("Foreign key violation {} not found in {}, cannot create {}",
                    fk.getDependentPropertyName(), fk.getMasterStorage().getResultSetName(), folioApiRecord.getJson().encodePrettily());
                logger.error(new JsonObject().encode());
                return new Resp (500, new JsonObject("{ \"message\": \"insert or update on table \\\"storage_table\\\" violates foreign key constraint \\\"fkey\\\"\", \"severity\": \"ERROR\", \"code\": \"23503\", \"detail\": \"Key (property value)=(the id) is not present in table \\\"a_referenced_table\\\".\", \"file\": \"ri_triggers.c\", \"line\": \"3266\", \"routine\": \"ri_ReportViolation\", \"schema\": \"diku_mod_inventory_storage\", \"table\": \"storage_table\", \"constraint\": \"a_fkey\" }").encodePrettily());
            } else {
                logger.debug("Found {} in {}",
                    folioApiRecord.getJson().getString(fk.getDependentPropertyName()), fk.getMasterStorage().getResultSetName());
            }
        }
        for (String mandatory : mandatoryProperties) {
            if (!folioApiRecord.getJson().containsKey(mandatory)) {
                return new Resp(422, new JsonObject("{\"message\" : {\n" + "      \"errors\" : [ {\n" + "        \"message\" : \"must not be null\",\n" + "        \"type\" : \"1\",\n" + "        \"code\" : \"javax.validation.constraints.NotNull.message\",\n" + "        \"parameters\" : [ {\n" + "          \"key\" : \"" + mandatory +"\",\n" + "          \"value\" : \"null\"\n" + "        } ]\n" + "      } ]\n" + "    }}").encodePrettily());
            }
        }
        return new Resp(201,"created");
    }

    protected int update (String id, FolioApiRecord folioApiRecord) {

        Resp validation = validateUpdate(id, folioApiRecord);
        if (validation.statusCode == 204) {
            records.put(id, folioApiRecord);
        }
        return validation.statusCode;
    }

    public Resp validateUpdate (String id, FolioApiRecord folioApiRecord) {
        if (failOnUpdate) {
            return new Resp(500, "forced fail on update");
        }
        if (folioApiRecord.hasId() && !id.equals(folioApiRecord.getId())) {
            return new Resp(400, "Fake record storage received request to update a record at an ID that doesn't match the ID in the record");
        }
        if (! records.containsKey(id)) {
            return new Resp(404,"Record not found, cannot update " + folioApiRecord.getJson().encodePrettily());
        }
        for (ForeignKey fk : masterEntities) {
            if (! folioApiRecord.getJson().containsKey(fk.getDependentPropertyName())) {
                return new Resp(422, "Foreign key violation, record must contain " + fk.getDependentPropertyName());
            }
            if (!fk.getMasterStorage().hasId(folioApiRecord.getJson().getString(fk.getDependentPropertyName()))) {
                return new Resp(500, "Not found: "+ folioApiRecord.getJson().getString(fk.getDependentPropertyName()) + " in " + fk.getMasterStorage().getResultSetName());
            }
        }
        return new Resp(204,"");
    }

    protected int delete (String id) {
        if (failOnDelete) return 500;
        if (!records.containsKey(id)) {
            logger.error("Record {} not found, cannot delete", id );
            return 404;
        }
        logger.debug("Dependent entities: {}", dependentEntities.size());
        for (ForeignKey fk : dependentEntities) {
            logger.debug("Deleting. Checking dependent {}", fk.getDependentStorage().getResultSetName());
            logger.debug("Looking at property {}", fk.getDependentPropertyName());
            if (fk.getDependentStorage().hasValue(fk.getDependentPropertyName(), id)) {
                logger.error("Foreign key violation, {} has a dependent record in {}",
                    records.get(id).getJson().encode(), fk.getDependentStorage().getResultSetName());
                return 400;
            }
        }
        records.remove(id);
        return 200;
    }

    protected Collection<FolioApiRecord> getRecords () {
        return records.values();
    }

    protected FolioApiRecord getRecord (String id) {
        if (failOnGetRecordById) {
            return null;
        } else {
            FolioApiRecord folioApiRecord = records.get( id );
            if (folioApiRecord != null) {
                folioApiRecord.setVersion(folioApiRecord.getVersion() + 1);
            }
            return folioApiRecord;
        }
    }

    // FOREIGN KEY HANDLING
    protected boolean hasId (String id) {
        return records.containsKey(id);
    }

    /**
     * Checks if this storage has a record where this property (presumably a foreign key property) has this value
     * @param fkPropertyName name of the referenced property
     * @param value value of the referenced property
     * @return true if value exists
     */
    protected boolean hasValue (String fkPropertyName, String value) {
        for (FolioApiRecord folioApiRecord : records.values()) {
            logger.debug("Checking {} for value {}", folioApiRecord.getJson().toString(), value);
            if (folioApiRecord.getJson().containsKey(fkPropertyName) && folioApiRecord.getJson().getString(fkPropertyName).equals(value)) {
                return true;
            }
        }
        return false;
    }

    // USED BY A DEPENDENT ENTITY TO SET UP ITS FOREIGN KEYS BY CALLS to acceptDependant()
    protected void declareDependencies() {}

    // METHOD ON THE PRIMARY KEY ENTITY TO REGISTER DEPENDENT ENTITIES
    protected void acceptDependant(RecordStorage dependentEntity, String dependentPropertyName) {
        ForeignKey fk = new ForeignKey(dependentEntity, dependentPropertyName, this);
        dependentEntities.add(fk);
        dependentEntity.setMasterEntity(fk);
    }

    protected void setMasterEntity (ForeignKey fk) {
        masterEntities.add(fk);
    }

    protected void declareMandatoryProperties () {}

    protected void declareUniqueProperties () {}
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
        FolioApiRecord folioApiRecord = getRecord(id);

        if (folioApiRecord != null) {
            respond(routingContext, folioApiRecord.getJson(), 200);
        } else {
            respondWithMessage(routingContext, (failOnGetRecordById ? "Forced error on get from " : "No record with ID " + id + " in ") + storageName, 404);
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
            respondWithMessage(routingContext, (failOnDelete ? "Forced " : "") + "Error deleting from " + storageName, code);
        }
    }

    /**
     * Handles DELETE ALL
     */
    protected void deleteAll (RoutingContext routingContext) {
        records.clear();
        respond(routingContext, new JsonObject("{\"message\": \"all records deleted\"}"), 200);
    }

    public void wipeMockRecords() {
        records.clear();
    }

    /**
     * Handles POST
     *
     */
    protected void createRecord(RoutingContext routingContext) {
        JsonObject recordJson = new JsonObject(routingContext.body().asString());
        StorageResponse response = insert(new FolioApiRecord(recordJson));
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
        JsonObject recordJson = new JsonObject(routingContext.body().asString());
        String id = routingContext.pathParam("id");
        int code = update(id, new FolioApiRecord(recordJson));
        if (code == 204) {
            respond(routingContext, code);
        } else {
            respondWithMessage(routingContext, (failOnUpdate ? "Forced " : "") + "Error updating record in " + storageName, code);
        }
    }

    // HELPERS FOR RESPONSE PROCESSING

    JsonObject buildJsonRecordsResponse(String optionalQuery) {
        if (failOnGetRecords) return null;
        JsonObject response = new JsonObject();
        JsonArray jsonRecords = new JsonArray();
        getRecords().forEach( folioApiRecord -> {
            if (optionalQuery == null || folioApiRecord.match(optionalQuery)) {
                jsonRecords.add(folioApiRecord.getJson());
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
