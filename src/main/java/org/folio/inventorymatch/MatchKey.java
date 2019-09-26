package org.folio.inventorymatch;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class MatchKey {
  
  private final Logger logger = LoggerFactory.getLogger("inventory-matcher-match-key");

  private final JsonObject candidateInstance;
  private final String matchKey;
  
  public MatchKey(JsonObject candidateInstance) {
    this.candidateInstance = candidateInstance;
    matchKey = buildMatchKey();
  }
  
  /**
   * Creates a match key from instance properties title, date of publication,
   * and physical description -- unless a matchKey is already provided in the
   * Instance object
   * @return a matchKey for the Instance
   */
  private String buildMatchKey() {
    StringBuilder key = new StringBuilder();
    if (candidateInstance.containsKey("matchKey") &&
        candidateInstance.getValue("matchKey") instanceof String &&
        candidateInstance.getString("matchKey") != null) {
      // use provided match key if any
      key.append(candidateInstance.getString("matchKey"));
    } else {
      // build match key from plain Instance properties
      key.append(candidateInstance.getString("title").toLowerCase())
         .append(getDateOfPublication())
         .append(getPhysicalDescription())
         .append(getPublisher());
    }
    logger.debug("Match key is:" + key.toString());
    return key.toString();
  }
  
    /**
   * Gets first occurring date of publication
   * @return one date of publication (empty string if none found)
   */
  private String getDateOfPublication() {
    String dateOfPublication = null;
    JsonArray publication = candidateInstance.getJsonArray("publication");
    if (publication != null && publication.getList().size()>0 ) {
      dateOfPublication = publication.getJsonObject(0).getString("dateOfPublication");
    }
    return dateOfPublication != null ? dateOfPublication : "";
  }

  /**
   * Gets first occurring physical description
   * @return one physical description (empty string if none found)
   */
  public String getPhysicalDescription() {
    String physicalDescription = null;
    JsonArray physicalDescriptions = candidateInstance.getJsonArray("physicalDescriptions");
    if (physicalDescriptions != null && physicalDescriptions.getList().size() >0) {
      physicalDescription = physicalDescriptions.getList().get(0).toString();
    }
    return physicalDescription != null ? physicalDescription : "";
  }
  
  public String getPublisher() {
    String publisher = null;
    JsonArray publication = candidateInstance.getJsonArray("publication");
    if (publication != null && publication.getList().size()>0 ) {
      publisher = publication.getJsonObject(0).getString("publisher");
    }
    return publisher != null ? publisher : "";
  }

  public String getKey () {
    return this.matchKey;
  }

}
