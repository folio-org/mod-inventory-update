package org.folio.inventoryupdate.importing.service.fileimport.upsertclient;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.updating.UpdateRequest;
import org.folio.tlib.util.TenantUtil;

public class InternalInventoryDeleteRequest extends UpdateRequest {

  private final JsonObject requestBody;

  public InternalInventoryDeleteRequest (Vertx vertx, RoutingContext routingContext, JsonObject jsonBody) {
    this.vertx = vertx;
    this.routingContext = routingContext;
    this.tenant = TenantUtil.tenant(routingContext);
    this.requestBody = jsonBody;
    this.request = routingContext.request();
    this.requestPath = request.path();
  }

  @Override
  public JsonObject bodyAsJson() {
    return requestBody;
  }

  @Override
  public String bodyAsString() {
    throw new UnsupportedOperationException("Only body as JSON in internal delete request.");
  }

  @Override
  public String queryParam(String paramName) {
    throw new UnsupportedOperationException("No query params for internal delete request.");
  }
}
