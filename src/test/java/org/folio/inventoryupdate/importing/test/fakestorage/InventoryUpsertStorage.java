package org.folio.inventoryupdate.importing.test.fakestorage;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.importing.test.fixtures.Files;

import java.util.Collection;


@SuppressWarnings("java:S2925") // don't use Thread sleep to wait for outcome
public class InventoryUpsertStorage extends RecordStorage {
    protected void inventoryBatchUpsertHrid (RoutingContext routingContext) {
        JsonObject upsertBody = routingContext.body().asJsonObject();
        String source = upsertBody.getJsonArray("inventoryRecordSets").getJsonObject(0).getJsonObject("instance").getString("source");
        try {
            upsertInventoryRecords(routingContext);
            Thread.sleep(500); // Fake some response time
        } catch (InterruptedException e) {
            logger.error("Fake response time interrupted");
            Thread.currentThread().interrupt();
        }
        if (source.endsWith("200")) {
            respond(routingContext, Files.JSON_SINGLE_RECORD_UPSERT_RESPONSE_200, 200);
        } else if (source.endsWith("207")) {
            respond(routingContext, Files.JSON_SINGLE_RECORD_UPSERT_RESPONSE_207, 207);
        } else {
            respond(routingContext, new JsonObject("{ \"testSetupProblem\": true}"), 500);
        }
    }

    @Override
    protected void deleteRecord (RoutingContext routingContext) {
        JsonObject deletionBody = routingContext.body().asJsonObject();
        final String id = deletionBody.getString("hrid");
        int code = delete(id);

        if (code == 200) {
            respond(routingContext, Files.JSON_SINGLE_RECORD_UPSERT_RESPONSE_200, 200);
        } else if (code == 404) {
            respondWithMessage(routingContext, "Not found", 404);
        } else {
            respondWithMessage(routingContext, (failOnDelete ? "Forced " : "") + "Error deleting from " + storageName, code);
        }
    }

    protected void upsertInventoryRecords(RoutingContext routingContext) {
        JsonObject recordsJson = new JsonObject(routingContext.body().asString());
        recordsJson.getJsonArray("inventoryRecordSets").stream()
                .forEach(theRecord -> {
                    JsonObject instance = ((JsonObject) theRecord).getJsonObject("instance");
                    String hrid = instance.getString("hrid");
                    instance.put("id", hrid); // dummy 'id'
                    FolioApiRecord incoming = new FolioApiRecord(instance);
                    FolioApiRecord existing = getRecord(instance.getString("hrid"));
                    if (existing == null) {
                        insert(new FolioApiRecord(instance));
                    } else {
                        update(incoming.getId(), incoming);
                    }
                });
    }

    public Collection<FolioApiRecord> internalGetInstances() {
        return getRecords();
    }

    @Override
    protected String getResultSetName() {
        return null;
    }

}

