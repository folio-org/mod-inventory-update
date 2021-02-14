package org.folio.inventoryupdate;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;


public class HridQuery implements InventoryQuery {
    private String hrid;
    private String queryString;

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
    String encodedQuery = queryString;
    try {
      encodedQuery = URLEncoder.encode(queryString,"UTF-8");
    } catch (UnsupportedEncodingException unsupportedEncodingException) {
        // ignore
    }
    return encodedQuery;
  }

}
