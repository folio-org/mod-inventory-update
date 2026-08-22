package org.folio.inventoryupdate.updating.service;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.validation.ValidatedRequest;
import org.folio.inventoryupdate.updating.UpdateRequest;

public class RequestValidated extends UpdateRequest {
  private final ValidatedRequest validatedRequest;

  public RequestValidated(Vertx vertx, RoutingContext routingContext, WebClient webClient) {
    this.vertx = vertx;
    this.routingContext = routingContext;
    this.validatedRequest = routingContext.get(RouterBuilder.KEY_META_DATA_VALIDATED_REQUEST);
    this.request = routingContext.request();
    this.requestPath = request.path();
    this.webClient = webClient;
  }

  @Override
  public String queryParam(String paramName) {
    if (validatedRequest.getQuery().get(paramName) != null) {
      return validatedRequest.getQuery().get(paramName).getString();
    } else {
      return null;
    }
  }

  @Override
  public JsonObject bodyAsJson() {
    if (validatedRequest.getBody() != null && validatedRequest.getBody().getJsonObject() != null) {
        return validatedRequest.getBody().getJsonObject();
    }
    return new JsonObject();
  }

  @Override
  public String bodyAsString() {
    if (validatedRequest.getBody() != null && validatedRequest.getBody().getString() != null) {
        return validatedRequest.getBody().getString();
    }
    return "";
  }

}
