package org.folio.inventoryimport.service;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;


import org.folio.inventoryimport.moduledata.database.ModuleStorageAccess;


public abstract class ServiceRequest {

    protected Vertx vertx;
    protected RoutingContext routingContext;
    protected String tenant;
    protected HttpServerRequest request;
    protected String requestPath;

    public Vertx vertx() {
        return vertx;
    }

    public String tenant () {
        return tenant;
    }

    public abstract JsonObject bodyAsJson();

    public abstract String bodyAsString();

    public ModuleStorageAccess moduleStorageAccess() {
        return new ModuleStorageAccess(vertx, tenant);
    }

    public abstract String queryParam(String paramName);

    public String queryParam(String paramName, String defaultValue) {
        return queryParam(paramName) == null ? defaultValue : queryParam(paramName);
    }

    public String getHeader(String headerName) {
        return request.getHeader(headerName);
    }

    public String absoluteURI () {
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

}
