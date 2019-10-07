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
    String keyStr = "";
    StringBuilder key = new StringBuilder();
    if (hasMatchKeyAsString(candidateInstance)) {
      // use provided match key if any
      key.append(candidateInstance.getString("matchKey"));
    } if (hasMatchKeyObject(candidateInstance)) {
      // build match key from match key object's properties
      String title = getInstanceMatchKeyValue("title") + " " + getInstanceMatchKeyValue("remainder-of-title");

      key.append(get70chars(title))
         .append(getInstanceMatchKeyValue("medium"))
         .append(getInstanceMatchKeyValue("name-of-part-section-of-work"))
         .append(getInstanceMatchKeyValue("number-of-part-section-of-work"))
         .append(getInstanceMatchKeyValue("inclusive-dates"))
         .append(getDateOfPublication())
         .append(getPhysicalDescription())
         .append(getPublisher());
    } else {
      // build match key from plain Instance properties
      key.append(get70chars(getTitle()))
         .append(getDateOfPublication())
         .append(getPhysicalDescription())
         .append(getPublisher());
    }
    keyStr = key.toString().trim().replace(" ", "_");
    logger.debug("Match key is:" + keyStr);
    return keyStr;
  }

  private String getTitle() {
    String title = null;
    if (candidateInstance.containsKey("title")) {
      title = candidateInstance.getString("title");
      title = stripTrimLowercase(title);
    }
    return title;
  }

  private static String get70chars (String input) {
    String output = "";
    if (input.length()<70) {
      output = String.format("%-70s", input).replace(" ", "_");
    } else {
      output = input.substring(0,45);
      String[] rest = input.substring(44).split("[ ]+");
      for (int i=0; i<rest.length; i++) {
        if (output.length()<70 && rest[i].length()>0) {
          output = output + rest[i].substring(0,1);
        } else {
          break;
        }
      }
      if (output.length()<70) {
        output = String.format("%-70s", output).replace(" ", "_");
      }
    }
    return output;
  }


  private static String stripTrimLowercase(String input) {
    String output = null;
    if (input != null) {
      input = input.replaceFirst("^[aA][ ]+", "");
      input = input.replaceFirst("^[aA]n[ ]+", "");
      input = input.replaceFirst("^[tT]he[ ]+", "");
      input = input.replaceAll("['{}]", "");
      input = input.replace("&", "and");
      output = input.replaceAll("[#\\*\\$@<>\\[\\]\"\\\\,.?:()=^~|-]", " ").trim().toLowerCase();
    }
    return output;
  }

  private String getInstanceMatchKeyValue(String name) {
    String value = null;
    if (hasMatchKeyObject(candidateInstance)) {
      value = candidateInstance.getJsonObject("matchKey").getString(name);
      value = stripTrimLowercase(value);
    }
    return value != null ? value : "";
  }

  private boolean hasMatchKeyObject (JsonObject instance) {
    return (instance.containsKey("matchKey")
            && candidateInstance.getValue("matchKey") instanceof JsonObject);
  }

  private boolean hasMatchKeyAsString (JsonObject instance) {
    return (instance.containsKey("matchKey")
            && candidateInstance.getValue("matchKey") instanceof String
            && candidateInstance.getString("matchKey") != null);
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
      dateOfPublication = dateOfPublication.replaceAll("\\D+","");
    }
    return dateOfPublication != null && dateOfPublication.length()>=4 ? " "+dateOfPublication.substring(dateOfPublication.length()-4) : "";
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
    physicalDescription = stripTrimLowercase(physicalDescription);
    return physicalDescription != null ? " " + physicalDescription : "";
  }

  public String getPublisher() {
    String publisher = null;
    JsonArray publication = candidateInstance.getJsonArray("publication");
    if (publication != null && publication.getList().size()>0 ) {
      publisher = publication.getJsonObject(0).getString("publisher");
    }
    publisher = stripTrimLowercase(publisher);
    return publisher != null ? " " + publisher : "";
  }

  public String getKey () {
    return this.matchKey;
  }

}
