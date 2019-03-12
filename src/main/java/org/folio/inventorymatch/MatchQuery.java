/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventorymatch;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * MatchQuery takes an Instance and constructs a Inventory query needed for
 * determining if a similar Instance already exists in Inventory
 *
 */
public class MatchQuery {

  private final Logger logger = LoggerFactory.getLogger("inventory-matcher-match-query");
  private final JsonObject candidateInstance;
  private final String queryString;

  public MatchQuery(JsonObject candidateInstance) {
    this.candidateInstance = candidateInstance;
    queryString = buildMatchQuery();
  }

  /**
   * Builds a match query from properties of incoming Instance
   * @return match query string
   */
  private String buildMatchQuery() {
    StringBuilder query = new StringBuilder();
    // Get match properties from request
    String title = candidateInstance.getString("title");
    logger.info("Title to match by is:" + title);
    query.append("(title=\"").append(title).append("\")");

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
}
