package org.folio.inventoryupdate.test;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public abstract class RecordStorage {
    public final static String TOTAL_RECORDS = "totalRecords";

    protected final Map<String,InventoryRecord> records = new HashMap<>();
    private final Logger logger = LoggerFactory.getLogger("fake-inventory-storage");

    public int insert (InventoryRecord record) {
        if (!record.hasId()) {
            record.generateId();
        }
        if (records.containsKey(record.getId())) {
            logger.error("Fake record storage already contains a record with id " + record.getId() + ", cannot create " + record.getJson().encodePrettily());
            return 400;
        }
        records.put(record.getId(), record);
        return 201;
    }

    public int update (String id, InventoryRecord record) {
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

    public Collection<InventoryRecord> getRecords () {
        return records.values();
    }

    public Stream<InventoryRecord> getRecordStream () {
        return getRecords().stream();
    }

    public InventoryRecord getRecord (String id) {
        return records.get(id);
    }

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


}
