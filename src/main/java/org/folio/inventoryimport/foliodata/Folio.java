package org.folio.inventoryimport.foliodata;

import io.vertx.ext.web.RoutingContext;
import org.folio.okapi.common.OkapiClient;
import org.folio.okapi.common.WebClientFactory;

import java.util.HashMap;
import java.util.Map;

public class Folio {
    public static OkapiClient okapiClient(RoutingContext ctx) {
        OkapiClient client = new OkapiClient(WebClientFactory.getWebClient(ctx.vertx()), ctx);
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-type", "application/json");
        if (ctx.request().getHeader("X-Okapi-Tenant") != null) headers.put("X-Okapi-Tenant", ctx.request().getHeader("X-Okapi-Tenant"));
        if (ctx.request().getHeader("X-Okapi-Token") != null) headers.put("X-Okapi-Token", ctx.request().getHeader("X-Okapi-Token"));
        headers.put("Accept", "application/json, text/plain");
        client.setHeaders(headers);
        return client;
    }

}
