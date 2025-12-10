package org.folio.inventoryupdate.updating;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.okapi.common.OkapiClient;

public abstract class UpdateRequest {
  protected Vertx vertx;
  protected RoutingContext routingContext;
  protected String tenant;
  protected HttpServerRequest request;
  protected String requestPath;

  public abstract JsonObject bodyAsJson();

  public abstract String bodyAsString();

  public abstract String queryParam(String paramName);

  public OkapiClient getOkapiClient() {
    return InventoryStorage.getOkapiClient(routingContext);
  }

  public RoutingContext routingContext() {
    return routingContext;
  }

  public String requestParam(String paramName) {
    return request.getParam(paramName);
  }

}
