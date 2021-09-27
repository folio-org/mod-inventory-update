package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

/**
 * Holds identifiers required for processing an Instance deletion request in a shared inventory
 *
 * @author ne
 */
public class DeletionIdentifiers {

  private static final String OAI_IDENTIFIER = "oaiIdentifier";
  private static final String LOCAL_IDENTIFIER = "localIdentifier";
  private static final String IDENTIFIER_TYPE_ID = "identifierTypeId";
  private static final String INSTITUTION_ID = "institutionId";

  JsonObject deletionRequestBody; 
  String localIdentifier = null;
  
  public DeletionIdentifiers (JsonObject deletionRequestBody) {
    this.deletionRequestBody = deletionRequestBody;
    if (deletionRequestBody.containsKey(OAI_IDENTIFIER)) {
      String oaiId = deletionRequestBody.getString(OAI_IDENTIFIER);
      this.localIdentifier = (oaiId != null ? oaiId.substring(oaiId.lastIndexOf(":")+1) : null);
    } else if (deletionRequestBody.containsKey(LOCAL_IDENTIFIER)) {
      this.localIdentifier = deletionRequestBody.getString(LOCAL_IDENTIFIER);
    }
  }
  
  public String localIdentifier () {
    return localIdentifier;
  }
  
  public String identifierTypeId () {
    return deletionRequestBody.getString(IDENTIFIER_TYPE_ID);
  }
  
  public String institutionId () {
    return deletionRequestBody.getString(INSTITUTION_ID);
  }
}
