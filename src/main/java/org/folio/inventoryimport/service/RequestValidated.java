package org.folio.inventoryimport.service;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.validation.ValidatedRequest;
import org.folio.tlib.util.TenantUtil;

public class RequestValidated extends ServiceRequest {


    private final ValidatedRequest validatedRequest;
    public RequestValidated(Vertx vertx, RoutingContext routingContext) {
        this.vertx = vertx;
        this.routingContext = routingContext;
        this.validatedRequest = routingContext.get(RouterBuilder.KEY_META_DATA_VALIDATED_REQUEST);
        this.tenant = TenantUtil.tenant(routingContext);
        this.request = routingContext.request();
        this.requestPath = request.path();
    }

    public String queryParam(String paramName) {
        if (validatedRequest.getQuery().get(paramName) != null) {
            return validatedRequest.getQuery().get(paramName).getString();
        } else {
            return null;
        }
    }

    public JsonObject bodyAsJson() {
        if (validatedRequest.getBody() != null) {
            if (validatedRequest.getBody().getJsonObject() != null) {
                return validatedRequest.getBody().getJsonObject();
            }
        }
        return new JsonObject();
    }

    public String bodyAsString() {
        if (validatedRequest.getBody() != null) {
            if (validatedRequest.getBody().getJsonObject() != null) {
                return validatedRequest.getBody().getString();
            }
        }
        return "";
    }
}
