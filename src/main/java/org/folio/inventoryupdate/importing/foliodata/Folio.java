package org.folio.inventoryupdate.importing.foliodata;

import io.vertx.ext.web.RoutingContext;
import java.util.HashMap;
import java.util.Map;
import org.folio.okapi.common.OkapiClient;
import org.folio.okapi.common.WebClientFactory;
import org.folio.okapi.common.XOkapiHeaders;

public final class Folio {

  private Folio() {
    throw new IllegalStateException("Utility class");
  }

  public static OkapiClient okapiClient(RoutingContext ctx) {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-type", "application/json");
    if (ctx.request().getHeader("X-Okapi-Tenant") != null) {
      headers.put("X-Okapi-Tenant", ctx.request().getHeader("X-Okapi-Tenant"));
    }
    if (ctx.request().getHeader(XOkapiHeaders.TOKEN) != null) {
      headers.put(XOkapiHeaders.TOKEN, ctx.request().getHeader(XOkapiHeaders.TOKEN));
    }
    headers.put("Accept", "application/json, text/plain");
    OkapiClient client = new OkapiClient(WebClientFactory.getWebClient(ctx.vertx()), ctx);
    client.setHeaders(headers);
    return client;
  }
}
