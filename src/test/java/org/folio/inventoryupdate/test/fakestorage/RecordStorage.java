package org.folio.inventoryupdate.test.fakestorage;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.test.fakestorage.entitites.InventoryRecord;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;

public abstract class RecordStorage {
    public final static String TOTAL_RECORDS = "totalRecords";
    // Property keys, JSON responses
    public final static String INSTANCES = "instances";
    public static final String HOLDINGS_RECORDS = "holdingsRecords";
    public static final String ITEMS = "items";
    public static final String INSTANCE_RELATIONSHIPS = "instanceRelationships";
    public static final String PRECEDING_SUCCEEDING_TITLES = "precedingSucceedingTitles";
    public static final String LOCATIONS = "locations";

    List<ForeignKey> dependentEntities = new ArrayList<>();
    List<ForeignKey> masterEntities = new ArrayList<>();

    protected FakeInventoryStorage fakeStorage;
    protected String resultSetName = null;

    protected final Map<String, InventoryRecord> records = new HashMap<>();
    private final Logger logger = LoggerFactory.getLogger("fake-inventory-storage");

    public void attachToFakeStorage(FakeInventoryStorage fakeStorage) {
        this.fakeStorage = fakeStorage;
        declareDependencies();
    }

    // PROPERTY NAME OF THE OBJECT THAT API RESULTS ARE RETURNED IN, IMPLEMENTED PER STORAGE ENTITY
    protected abstract String getResultSetName();

    // INTERNAL DATABASE OPERATIONS - insert() IS DECLARED PUBLIC SO THE TEST SUITE CAN INITIALIZE DATA OUTSIDE THE API.
    public int insert (InventoryRecord record) {
        if (!record.hasId()) {
            record.generateId();
        }
        if (records.containsKey(record.getId())) {
            logger.error("Fake record storage already contains a record with id " + record.getId() + ", cannot create " + record.getJson().encodePrettily());
            return 400;
        }
        logger.debug("Checking foreign keys");
        logger.debug("Got " + masterEntities.size() + " foreign keys");
        for (ForeignKey fk : masterEntities) {
            if (! record.getJson().containsKey(fk.getDependentPropertyName())) {
                logger.error("Foreign key violation, record must contain " + fk.getDependentPropertyName());
                return 400;
            }
            if (!fk.getMasterStorage().hasId(record.getJson().getString(fk.getDependentPropertyName()))) {
                logger.error("Foreign key violation " + fk.getDependentPropertyName() + " not found in "+ fk.getMasterStorage().getResultSetName() + ", cannot create " + record.getJson().encodePrettily());
                return 400;
            } else {
                logger.debug("Found " + record.getJson().getString(fk.getDependentPropertyName()) + " in " + fk.getMasterStorage().getResultSetName());
            }

        }
        records.put(record.getId(), record);
        return 201;
    }

    protected int update (String id, InventoryRecord record) {
        if (!record.hasId()) {
            record.setId(id);
        } else if (!id.equals(record.getId())) {
            logger.error("Fake record storage received request to update a record at an ID that doesn't match the ID in the record");
            return 400;
        }
        if (! records.containsKey(id)) {
            logger.error("Record not found, cannot update " + record.getJson().encodePrettily());
            return 404;
        }
        records.put(id, record);
        return 204;
    }

    protected int delete (String id) {
        if (!records.containsKey(id)) {
            logger.error("Record " + id + "not found, cannot delete");
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
        return records.get(id);
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

    // API REQUEST HANDLERS

    /**
     * Handles GET request with query parameter
     * @param routingContext
     */
    public void getRecords(RoutingContext routingContext) {
        final String optionalQuery = routingContext.request().getParam("query") != null ?
                decode(routingContext.request().getParam("query")) : null;
        routingContext.request().endHandler(res -> {
            respond(routingContext, buildJsonRecordsResponse(optionalQuery), 200);
        });

        routingContext.request().exceptionHandler(res -> {
            respondWithMessage(routingContext, res);
        });
    }

    /**
     * Handles GET by ID
     * @param routingContext
     */
    protected void getRecordById(RoutingContext routingContext) {
        final String id = routingContext.pathParam("id");
        InventoryRecord record = getRecord(id);

        routingContext.request().endHandler(res -> {
            respond(routingContext, record.getJson(), 200);
        });
        routingContext.request().exceptionHandler(res -> {
            respondWithMessage(routingContext, res);
        });
    }

    /**
     * Handles DELETE
     * @param routingContext
     */
    protected void deleteRecord (RoutingContext routingContext) {
        final String id = routingContext.pathParam("id");
        int code = delete(id);

        routingContext.request().endHandler(res -> {
            respond(routingContext, new JsonObject(), code);
        });
        routingContext.request().exceptionHandler(res -> {
            respondWithMessage(routingContext, res);
        });

    }

    // API REQUEST HANDLERS TO BE IMPLEMENTED PER STORAGE ENTITY

    /**
     * Handles POST
     * @param routingContext
     */
    protected abstract void createRecord (RoutingContext routingContext);

    /**
     * Handles PUT
     * @param routingContext
     */
    protected abstract void updateRecord (RoutingContext routingContext);


    // HELPERS FOR RESPONSE PROCESSING

    private JsonObject buildJsonRecordsResponse(String optionalQuery) {
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
     * @param routingContext
     * @param responseJson
     * @param code
     */
    protected static void respond(RoutingContext routingContext, JsonObject responseJson, int code) {
        routingContext.response().headers().add("Content-Type", "application/json");
        routingContext.response().setStatusCode(code);
        routingContext.response().end(responseJson.encodePrettily());
    }

    /**
     * Respond with status code
     * @param routingContext
     * @param code
     */
    protected static void respond(RoutingContext routingContext, int code) {
        routingContext.response().headers().add("Content-Type", "application/json");
        routingContext.response().setStatusCode(code);
        routingContext.response().end();
    }

    /**
     * Respond with text message (error response)
     * @param routingContext
     * @param res
     */
    protected static void respondWithMessage(RoutingContext routingContext, Throwable res) {
        routingContext.response().setStatusCode(500);
        routingContext.response().end(res.getMessage());
    }


    // UTILS

    public static String decode (String string) {
        try {
            return URLDecoder.decode(string, "UTF-8");
        } catch (UnsupportedEncodingException uee) {
            return "";
        }

    }

    public static String encode (String string) {
        try {
            return URLEncoder.encode(string, "UTF-8");
        } catch (UnsupportedEncodingException uee) {
            return "";
        }
    }

    public void logRecords (Logger logger) {
        records.values().stream().forEach(record -> logger.debug(record.getJson().encodePrettily()));
    }

    public static class ForeignKey {

        private RecordStorage dependentStorage;
        private String dependentPropertyName;
        private RecordStorage masterStorage;

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
