package org.folio.inventoryupdate.importing.service;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;

public abstract class ServiceRequest {

  protected Vertx vertx;
  protected RoutingContext routingContext;
  protected String tenant;
  protected HttpServerRequest request;
  protected String requestPath;

  public Vertx vertx() {
    return vertx;
  }

  public String tenant() {
    return tenant;
  }

  public abstract JsonObject bodyAsJson();

  public abstract RequestBody body();

  public abstract String bodyAsString();

  public EntityStorage entityStorage() {
    return new EntityStorage(vertx, tenant);
  }

  public String dbSchema() {
    return entityStorage().schema();
  }

  public abstract String queryParam(String paramName);

  public String queryParam(String paramName, String defaultValue) {
    return queryParam(paramName) == null ? defaultValue : queryParam(paramName);
  }

  public String getHeader(String headerName) {
    return request.getHeader(headerName);
  }

  public String absoluteUri() {
    return request.absoluteURI();
  }

  public String path() {
    return requestPath;
  }

  public RoutingContext routingContext() {
    return routingContext;
  }

  public String requestParam(String paramName) {
    return request.getParam(paramName);
  }

  public UUID currentUser() {
    String userId = request.getHeader("X-Okapi-User-Id");
    if (userId == null) {
      return null;
    } else {
      try {
        return UUID.fromString(userId);
      } catch (IllegalArgumentException iae) {
        return null;
      }
    }
  }
}
