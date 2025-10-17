package org.folio.inventory.updating;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public abstract class InventoryQuery {
    protected String queryString;

    public String getURLEncodedQueryString( ) {
        return URLEncoder.encode(queryString, StandardCharsets.UTF_8);
    }
}
