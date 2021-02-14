/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventoryupdate;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * MatchQuery takes an Instance and constructs a Inventory query needed for
 * determining if a similar Instance already exists in Inventory
 *
 */
public class MatchQuery implements InventoryQuery {

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
    String encodedQuery = queryString;
    try {
      encodedQuery = URLEncoder.encode(queryString,"UTF-8");
    } catch (UnsupportedEncodingException unsupportedEncodingException) {
      // ignore
    }
    return encodedQuery;
  }

  public String getMatchKey () {
    return this.matchKey;
  }
}
