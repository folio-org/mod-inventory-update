package org.folio.inventoryupdate.importing.test.fakestorage.entities;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.importing.test.fakestorage.FolioApiRecord;
import org.folio.inventoryupdate.updating.MatchKey;

public class InputInstance extends FolioApiRecord {

  public static final String TITLE = "title";
  public static final String INSTANCE_TYPE_ID = "instanceTypeId";
  public static final String MATCH_KEY = "matchKey";
  public static final String HRID = "hrid";
  public static final String SOURCE = "source";
  public static final String IDENTIFIERS = "identifiers";
  public static final String PUBLICATION = "publication";
  public static final String DATE_OF_PUBLICATION = "dateOfPublication";
  public static final String CLASSIFICATIONS = "classifications";
  public static final String CLASSIFICATION_TYPE_ID = "classificationTypeId";
  public static final String CLASSIFICATION_NUMBER = "classificationNumber";
  public static final String CONTRIBUTORS = "contributors";
  public static final String CONTRIBUTOR_NAME_TYPE_ID = "contributorNameTypeId";
  public static final String PERSONAL_NAME_TYPE =  "2b94c631-fca9-4892-a730-03ee529ffe2a";
  public static final String PHYSICAL_DESCRIPTIONS = "physicalDescriptions";
  public static final String EDITIONS = "editions";

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
