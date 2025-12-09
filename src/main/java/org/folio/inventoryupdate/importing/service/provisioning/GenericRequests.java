package org.folio.inventoryupdate.importing.service.provisioning;

import static org.folio.okapi.common.HttpResponse.responseJson;
import static org.folio.okapi.common.HttpResponse.responseText;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.Entity;
import org.folio.inventoryupdate.importing.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryupdate.importing.moduledata.database.SqlQuery;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.tlib.postgres.PgCqlException;

public final class GenericRequests {

  private GenericRequests() {
    throw new IllegalStateException("Static storage utilities");
  }

  public static Future<Void> getEntity(ServiceRequest request, Entity entity) {
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.moduleStorageAccess().getEntity(id, entity).onSuccess(instance -> {
      if (instance == null) {
        responseText(request.routingContext(), 404).end(entity.entityName() + " " + id + " not found.");
      } else {
        responseJson(request.routingContext(), 200).end(instance.asJson().encodePrettily());
      }
    }).mapEmpty();
  }

  public static Future<Void> getEntities(ServiceRequest request, Entity entity) {
    ModuleStorageAccess db = request.moduleStorageAccess();
    SqlQuery query;
    try {
      query = entity.cqlToSql(request, db.schemaDotTable(entity.table()));
    } catch (PgCqlException pce) {
      responseText(request.routingContext(), 400)
          .end("Could not execute query to retrieve " + entity.jsonCollectionName() + ": " + pce.getMessage()
              + " Request:" + request.absoluteUri());
      return Future.succeededFuture();
    } catch (Exception e) {
      return Future.failedFuture(e.getMessage());
    }
    return db.getEntities(query.getQueryWithLimits(), entity).onComplete(result -> {
      if (result.succeeded()) {
        JsonObject responseJson = new JsonObject();
        JsonArray jsonRecords = new JsonArray();
        responseJson.put(entity.jsonCollectionName(), jsonRecords);
        List<Entity> recs = result.result();
        for (Entity rec : recs) {
          jsonRecords.add(rec.asJson());
        }
        db.getCount(query.getCountingSql()).onComplete(count -> {
          responseJson.put("totalRecords", count.result());
          responseJson(request.routingContext(), 200).end(responseJson.encodePrettily());
        });
      } else {
        responseText(request.routingContext(), 500).end("Problem retrieving jobs: " + result.cause().getMessage());
      }
    }).mapEmpty();
  }

  public static Future<Void> deleteEntity(ServiceRequest request, Entity entity) {
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.moduleStorageAccess().deleteEntity(id, entity).onSuccess(result -> {
      if (result == 0) {
        responseText(request.routingContext(), 404).end("Not found");
      } else {
        responseText(request.routingContext(), 200).end();
      }
    }).mapEmpty();
  }

  public static Future<Void> storeEntityRespondWith201(ServiceRequest request, Entity entity) {
    ModuleStorageAccess db = request.moduleStorageAccess();
    return db.storeEntity(entity.withCreatingUser(request.currentUser()))
        .onSuccess(id -> db.getEntity(id, entity).map(stored ->
            responseJson(request.routingContext(), 201).end(stored.asJson().encodePrettily()))).mapEmpty();
  }
}
