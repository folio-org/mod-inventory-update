/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventorymatch;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import io.vertx.core.json.JsonArray;
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
  private final String matchKey;
  private final String queryString;

  public MatchQuery(JsonObject candidateInstance) {
    this.candidateInstance = candidateInstance;
    matchKey = buildMatchKey();
    queryString = buildMatchQuery();
  }

  private String buildMatchKey() {
    StringBuilder key = new StringBuilder();
    key.append(candidateInstance.getString("title").toLowerCase())
       .append(getDateOfPublication())
       .append(getPhysicalDescription());
    logger.info("Match key is:" + key.toString());
    return key.toString();
  }

  /**
   * Builds a match query from properties of incoming Instance
   * @return match query string
   */
  private String buildMatchQuery() {
    StringBuilder query = new StringBuilder();
    // Get match properties from request
    query.append("(indexTitle=\"").append(matchKey).append("\")");
    return query.toString();
  }

  private String getDateOfPublication() {
    String dateOfPublication = null;
    JsonArray publication = candidateInstance.getJsonArray("publication");
    if (publication != null && publication.getList().size()>0 ) {
      dateOfPublication = publication.getJsonObject(0).getString("dateOfPublication");
    }
    return dateOfPublication != null ? dateOfPublication : "";
  }

  public String getPhysicalDescription() {
    String physicalDescription = null;
    JsonArray physicalDescriptions = candidateInstance.getJsonArray("physicalDescriptions");
    if (physicalDescriptions != null && physicalDescriptions.getList().size() >0) {
      physicalDescription = physicalDescriptions.getList().get(0).toString();
    }
    return physicalDescription != null ? physicalDescription : "";
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
