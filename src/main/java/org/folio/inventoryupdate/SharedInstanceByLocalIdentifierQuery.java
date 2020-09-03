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
 *
 * @author ne
 */
public class SharedInstanceByLocalIdentifierQuery implements InventoryQuery {

  private final String localIdentifier;
  private final String identifierTypeId;
  private final String queryString;

  private final Logger logger = LoggerFactory.getLogger("inventory-upsert-hrid-query");


  public SharedInstanceByLocalIdentifierQuery(String localIdentifier, String identifierTypeId) {
    this.localIdentifier = localIdentifier;
    this.identifierTypeId = identifierTypeId;
    queryString = buildQuery();
  }

  private String buildQuery() {
    StringBuilder query = new StringBuilder();
    query.append("(identifiers =/@value/@identifierTypeId=")
          .append("\"").append(identifierTypeId).append("\"")
          .append(" \"").append(localIdentifier).append("\"")
          .append(")");
    return query.toString();
  }

  @Override
  public String getQueryString() {
    return queryString;
  }

  @Override
  public String getURLEncodedQueryString() {
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
