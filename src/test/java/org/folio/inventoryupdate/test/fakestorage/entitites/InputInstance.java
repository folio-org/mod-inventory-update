package org.folio.inventoryupdate.test.fakestorage.entitites;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.MatchKey;

public class InputInstance extends InventoryRecord {

  public static String TITLE = "title";
  public static String INSTANCE_TYPE_ID = "instanceTypeId";
  public static String MATCH_KEY = "matchKey";
  public static String HRID = "hrid";
  public static String SOURCE = "source";
  public static String IDENTIFIERS = "identifiers";
  public static String PUBLICATION = "publication";
  public static String DATE_OF_PUBLICATION = "dateOfPublication";
  public static String CLASSIFICATIONS = "classifications";
  public static String CLASSIFICATION_TYPE_ID = "classificationTypeId";
  public static String CLASSIFICATION_NUMBER = "classificationNumber";
  public static String CONTRIBUTORS = "contributors";
  public static String CONTRIBUTOR_NAME_TYPE_ID = "contributorNameTypeId";
  public static String MEETING_NAME_TYPE = "e8b311a6-3b21-43f2-a269-dd9310cb2d0a";
  public static String PERSONAL_NAME_TYPE =  "2b94c631-fca9-4892-a730-03ee529ffe2a";
  public static String CORPORATE_NAME_TYPE = "2e48e713-17f3-4c13-a9f8-23845bb210aa";
  public static String PHYSICAL_DESCRIPTIONS = "physicalDescriptions";
  public static String EDITIONS = "editions";

  public InputInstance() {
    super();
  }

  public InputInstance setTitle (String title) {
    recordJson.put(TITLE, title);
    return this;
  }

  public InputInstance setInstanceTypeId (String instanceTypeId) {
    recordJson.put(INSTANCE_TYPE_ID, instanceTypeId);
    return this;
  }

  public InputInstance setSource (String source) {
    recordJson.put(SOURCE, source);
    return this;
  }

  public InputInstance setDateOfPublication (String date)  {
    JsonObject publicationItem;
    JsonArray publication = recordJson.getJsonArray( PUBLICATION );

    if (publication != null && !publication.isEmpty()) {
      publicationItem = publication.getJsonObject( 0 );
    } else {
      publication = new JsonArray();
      publicationItem = new JsonObject();
      publication.add(publicationItem);
      recordJson.put(PUBLICATION, publication);
    }
    publicationItem.put(DATE_OF_PUBLICATION, date);
    return this;
  }

  public InputInstance setClassification (String classificationTypeId, String classificationNumber) {
    JsonArray classifications = new JsonArray();
    JsonObject classification = new JsonObject();
    classification.put(CLASSIFICATION_TYPE_ID, classificationTypeId);
    classification.put(CLASSIFICATION_NUMBER, classificationNumber);
    classifications.add(classification);
    recordJson.put( CLASSIFICATIONS, classifications);
    return this;
  }

  public InputInstance setContributor (String contributorNameTypeId, String name) {
    JsonArray contributors = new JsonArray();
    JsonObject contributor = new JsonObject();
    contributor.put(CONTRIBUTOR_NAME_TYPE_ID, contributorNameTypeId);
    contributor.put("name", name);
    contributors.add(contributor);
    recordJson.put(CONTRIBUTORS, contributors);
    return this;
  }

  public InputInstance setPhysicalDescription ( String description ) {
    JsonArray descriptions = new JsonArray();
    descriptions.add(description);
    recordJson.put(PHYSICAL_DESCRIPTIONS, descriptions);
    return this;
  }

  public InputInstance setEdition ( String edition ) {
    JsonArray editions = new JsonArray();
    editions.add(edition);
    recordJson.put( EDITIONS, editions);
    return this;
  }

  public InputInstance setMatchKeyAsString( String matchKey) {
    recordJson.put(MATCH_KEY, matchKey);
    return this;
  }

  public InputInstance setMatchKeyAsObject ( JsonObject matchKey ) {
    recordJson.put(MATCH_KEY, matchKey);
    return this;
  }

  public InputInstance generateMatchKey () {
    setMatchKeyAsString(new MatchKey(recordJson).getKey());
    return this;
  }

  public InputInstance setHrid (String hrid) {
    recordJson.put(HRID, hrid);
    return this;
  }

  public String getHrid () {
    return recordJson.getString(HRID);
  }

  public InputInstance setIdentifiers (JsonArray identifiers) {
    recordJson.put(IDENTIFIERS, identifiers);
    return this;
  }

}
