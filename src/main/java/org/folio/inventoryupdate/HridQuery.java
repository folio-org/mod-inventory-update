package org.folio.inventoryupdate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;


public class HridQuery extends InventoryQuery {
    private final String hrid;

    public HridQuery(String hrid) {
        this.hrid = hrid;
        queryString = buildHridQuery();
    }

    /**
   * Builds a match query from properties of incoming Instance
   * @return match query string
   */
  private String buildHridQuery() {
      // Get match properties from request
      return "(hrid==\"" + hrid + "\")";
  }

}
