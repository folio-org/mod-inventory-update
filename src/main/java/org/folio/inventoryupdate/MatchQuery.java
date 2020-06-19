/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventoryupdate;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * MatchQuery takes an Instance and constructs a Inventory query needed for
 * determining if a similar Instance already exists in Inventory
 *
 */
public class MatchQuery implements InventoryQuery {

  private final Logger logger = LoggerFactory.getLogger("inventory-update-match-query");
  private final String matchKey;
  private final String queryString;

  public MatchQuery(String matchKey) {
    this.matchKey = matchKey;
    queryString = buildMatchQuery();
  }

  /**
   * Builds a match query from properties of incoming Instance
   * @return match query string
   */
  private String buildMatchQuery() {
    StringBuilder query = new StringBuilder();
    // Get match properties from request
    query.append("(matchKey==\"").append(matchKey).append("\")");
    return query.toString();
  }

  /**
   *
   * @return Instance match query
   */
  public String getQueryString () {
    return queryString;
  }

  /**
   *
   * @return URL encoded Instance match query
   */
  public String getURLEncodedQueryString () {
    String encodedQuery;
    try {
      encodedQuery = URLEncoder.encode(queryString,"UTF-8");
    } catch (UnsupportedEncodingException unsupportedEncodingException) {
      logger.fatal("System error: Unsupported encoding of Inventory query" + unsupportedEncodingException);
      encodedQuery =  "encoding-error-while-building-query-string";
    }
    return encodedQuery;
  }

  public String getMatchKey () {
    return this.matchKey;
  }
}
