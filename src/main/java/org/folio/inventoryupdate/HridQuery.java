package org.folio.inventoryupdate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;


public class HridQuery implements InventoryQuery {
    private final String hrid;
    private final String queryString;

    public HridQuery(String hrid) {
        this.hrid = hrid;
        queryString = buildHridQuery();
    }

    /**
   * Builds a match query from properties of incoming Instance
   * @return match query string
   */
  private String buildHridQuery() {
    StringBuilder query = new StringBuilder();
    // Get match properties from request
    query.append("(hrid==\"").append(hrid).append("\")");
    return query.toString();
  }

  /**
   * 
   * @return un-encoded query string
   */
  public String getQueryString() {
    return this.queryString;
  }
  /**
   *
   * @return URL encoded Instance match query
   */
  public String getURLEncodedQueryString () {
    return URLEncoder.encode(queryString, StandardCharsets.UTF_8);
  }

}
