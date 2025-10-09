package org.folio.inventoryimport.service;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.tlib.util.TenantUtil;

public class RequestUnvalidated extends ServiceRequest {

    public RequestUnvalidated(Vertx vertx, RoutingContext routingContext) {
        this.vertx = vertx;
        this.routingContext = routingContext;
        this.tenant = TenantUtil.tenant(routingContext);
        this.request = routingContext.request();
        this.requestPath = request.path();
    }

    @Override
    public JsonObject bodyAsJson() {
        return routingContext.body().asJsonObject();
    }

    @Override
    public String bodyAsString() {
        return routingContext.body().asString();
    }

    @Override
    public String queryParam(String paramName) {
        return routingContext().request().getParam(paramName);
    }

}
