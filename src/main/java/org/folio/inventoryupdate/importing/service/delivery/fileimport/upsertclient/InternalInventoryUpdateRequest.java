package org.folio.inventoryupdate.importing.service.delivery.fileimport.upsertclient;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.folio.inventoryupdate.updating.UpdateRequest;

public class InternalInventoryUpdateRequest extends UpdateRequest {

  private final JsonObject requestBody;

  public InternalInventoryUpdateRequest(WebClient webClient, Vertx vertx, RoutingContext routingContext,
                                        JsonObject jsonBody) {
    this.vertx = vertx;
    this.routingContext = routingContext;
    this.requestBody = jsonBody;
    this.request = routingContext.request();
    this.requestPath = request.path();
    this.webClient = webClient;
  }

  @Override
  public JsonObject bodyAsJson() {
    return requestBody;
  }

  @Override
  public String bodyAsString() {
    throw new UnsupportedOperationException("Only body as JSON in internal update request.");
  }

  @Override
  public String queryParam(String paramName) {
    throw new UnsupportedOperationException("No query params for internal update request.");
  }
}
