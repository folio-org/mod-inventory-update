/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

/**
 * Holds identifiers required for processing an Instance deletion request in a shared inventory
 *
 * @author ne
 */
public class DeletionIdentifiers {
  
  JsonObject deletionRequestBody; 
  String localIdentifier;
  
  public DeletionIdentifiers (JsonObject deletionRequestBody) {
    this.deletionRequestBody = deletionRequestBody;
    if (deletionRequestBody.containsKey("oaiIdentifier")) {
      String oaiId = deletionRequestBody.getString("oaiIdentifier");
      this.localIdentifier = (oaiId != null ? oaiId.substring(oaiId.lastIndexOf(":")+1) : null);
    } else {
      this.localIdentifier = null;
    }
  }
  
  public String localIdentifier () {
    return localIdentifier;
  }
  
  public String identifierTypeId () {
    return deletionRequestBody.getString("identifierTypeId");
  }
  
  public String institutionId () {
    return deletionRequestBody.getString("institutionId");
  }
}
