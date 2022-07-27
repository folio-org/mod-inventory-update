package org.folio.inventoryupdate.entities;

import io.vertx.core.json.JsonObject;

/**
 * Holds identifiers required for processing record set deletion requests in a shared inventory:
 * the library's local record identifier and FOLIO's identifier type for local identifiers from the given library.
 *
 * @author ne
 */
public class RecordIdentifiers
{

  public static final String OAI_IDENTIFIER = "oaiIdentifier";
  public static final String LOCAL_IDENTIFIER = "localIdentifier";
  public static final String IDENTIFIER_TYPE_ID = "identifierTypeId";
  public static final String INSTITUTION_ID = "institutionId";

  JsonObject deletionRequestBody;
  String recordIdentifier = null;
  String identifierTypeId;
  String institutionId;

  private RecordIdentifiers () {}

  public static RecordIdentifiers identifiersFromDeleteRequestJson( JsonObject deletionRequestBody) {
    RecordIdentifiers identifiers = new RecordIdentifiers();
    identifiers.deletionRequestBody = deletionRequestBody;
    if (deletionRequestBody.containsKey(OAI_IDENTIFIER)) {
      String oaiId = deletionRequestBody.getString(OAI_IDENTIFIER);
      identifiers.recordIdentifier = localIdentifierFromOaiIdentifier(oaiId);
    } else if (deletionRequestBody.containsKey(LOCAL_IDENTIFIER)) {
      identifiers.recordIdentifier = deletionRequestBody.getString(LOCAL_IDENTIFIER);
    }
    identifiers.identifierTypeId = deletionRequestBody.getString( IDENTIFIER_TYPE_ID );
    identifiers.institutionId = deletionRequestBody.getString( INSTITUTION_ID );
    return identifiers;
  }

  public static RecordIdentifiers identifiersWithLocalIdentifier ( String institutionId, String identifierTypeId, String localIdentifier ) {
    RecordIdentifiers identifiers = new RecordIdentifiers();
    identifiers.institutionId = institutionId;
    identifiers.identifierTypeId = identifierTypeId;
    identifiers.recordIdentifier = localIdentifier;
    return identifiers;
  }

  private static String localIdentifierFromOaiIdentifier (String oaiId) {
    return  (oaiId != null ? oaiId.substring(oaiId.lastIndexOf(":")+1) : null);
  }

  public String localIdentifier () {
    return recordIdentifier;
  }

  public String identifierTypeId () {
    return identifierTypeId;
  }

  public String institutionId () {
    return institutionId;
  }
}
