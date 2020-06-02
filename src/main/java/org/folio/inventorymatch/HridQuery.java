package org.folio.inventorymatch;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class HridQuery implements InstanceQuery {
    private String hrid;
    private final Logger logger = LoggerFactory.getLogger("inventory-upsert-hrid-query");
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
    String encodedQuery;
    try {
      encodedQuery = URLEncoder.encode(queryString.toString(),"UTF-8");
    } catch (UnsupportedEncodingException unsupportedEncodingException) {
      logger.fatal("System error: Unsupported encoding of Inventory query" + unsupportedEncodingException);
      encodedQuery =  "encoding-error-while-building-query-string";
    }
    return encodedQuery;
  }

}