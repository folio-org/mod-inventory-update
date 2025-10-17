package org.folio.inventoryupdate.updating;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


public class MatchKey {

  private final Logger logger = LogManager.getLogger("inventory-update-match-key");

  private final JsonObject candidateInstance;
  private final String matchkee;
  private static final Map<String, String> typeOfMap;
  private static final String MATCH_KEY = "matchKey";
  private static final String TITLE = "title";

  private static final String PERSONAL_NAME_TYPE  = "2b94c631-fca9-4892-a730-03ee529ffe2a";
  private static final String CORPORATE_NAME_TYPE = "2e48e713-17f3-4c13-a9f8-23845bb210aa";
  private static final String MEETING_NAME_TYPE   = "e8b311a6-3b21-43f2-a269-dd9310cb2d0a";

  static {
    typeOfMap = new HashMap<>();
    // typeOfMap.put("6312d172-f0cf-40f6-b27d-9fa8feaf332f", "a");
    // typeOfMap.put("497b5090-3da2-486c-b57f-de5bb3c2e26d", "c");
    typeOfMap.put("497b5090-3da2-486c-b57f-de5bb3c2e26d", "d"); // notated music
    typeOfMap.put("526aa04d-9289-4511-8866-349299592c18", "e"); // cartographic image
    // typeOfMap.put("a2c91e87-6bab-44d6-8adb-1fd02481fc4f", "f");
    typeOfMap.put("535e3160-763a-42f9-b0c0-d8ed7df6e2a2", "g"); // still image
    typeOfMap.put("9bce18bd-45bf-4949-8fa8-63163e4b7d7f", "i"); // sounds
    typeOfMap.put("3be24c14-3551-4180-9292-26a786649c8b", "j"); // performed music
    // typeOfMap.put("a2c91e87-6bab-44d6-8adb-1fd02481fc4f", "k");
    typeOfMap.put("df5dddff-9c30-4507-8b82-119ff972d4d7", "m"); // computer dataset
    // typeOfMap.put("a2c91e87-6bab-44d6-8adb-1fd02481fc4f", "o");
    typeOfMap.put("a2c91e87-6bab-44d6-8adb-1fd02481fc4f", "p"); // other
    typeOfMap.put("c1e95c2b-4efc-48cf-9e71-edb622cf0c22", "r"); // three-dimensional form
    typeOfMap.put("6312d172-f0cf-40f6-b27d-9fa8feaf332f", "t"); // text
  }

  public MatchKey(JsonObject candidateInstance) {
    this.candidateInstance = candidateInstance;
    logger.debug("MatchKey: candidateInstance {}", candidateInstance::encodePrettily);
    matchkee = buildMatchKey();
  }

  /**
   * Creates a match key from instance properties title, date of publication,
   * and physical description -- unless a matchKey is already provided in the
   * Instance object
   * @return a matchKey for the Instance
   */
  private String buildMatchKey() {
    String keyStr;
    StringBuilder key = new StringBuilder();
    if (hasMatchKeyAsString(candidateInstance)) {
      // use provided match key if any
      key.append(candidateInstance.getString(MATCH_KEY));
    } else {
      if (hasMatchKeyObject(candidateInstance)) {
        // build match key from match key object's properties
        String title = getInstanceMatchKeyValue(TITLE) + " "
            + getInstanceMatchKeyValue("remainder-of-title");
        key.append(get70chars(title))
           .append(get5chars(getInstanceMatchKeyValue("medium")));
      } else {
        // build match key from plain Instance properties
        key.append(get70chars(getTitle()));
      }
      key.append(getDateOfPublication())
          .append(getPhysicalDescription())
          .append(getEdition())
          .append(getPublisher())
          .append(getTypeOf())
          .append(getTitlePart())
          .append(getTitleNumber())
          .append(getAuthor())
          .append(getTitleDates())
          .append(getGovDocNumber())
          .append(getFormatChar());
    }
    keyStr = key.toString().trim().replace(" ", "_");
    logger.debug("Match key is: {}", keyStr);
    return keyStr;
  }

  private String getTitle() {
    String title = "";
    if (candidateInstance.containsKey(TITLE)) {
      title = candidateInstance.getString(TITLE);
      title = unaccent(title);
      title = stripTrimLowercase(title);
    }
    return title;
  }

  private static String get70chars (String input) {
    String output = "";
    if (input != null && input.length()<70) {
      output = String.format("%-70s", input).replace(" ", "_");
    } else if (input != null) {
      output = input.substring(0,45);
      String[] rest = input.substring(45).split("[ ]+");
      for (int i=0; i<rest.length; i++) {
        if (output.length()<70) {
          output = output + (rest[i].length()>0 ? rest[i].substring(0,1) : "");
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
      output = input.replaceAll("[#*$@<>\\[\\]\"\\\\,.?:()=^~|©;`-]", " ").trim().toLowerCase();
    }
    return output;
  }


  private static String unaccent(String str) {
    return (str == null ? str :
            Normalizer.normalize(str, Normalizer.Form.NFD)
                    .replaceAll("[^\\p{ASCII}]", ""));
  }

  private static final List<Pattern> CONTIGUOUS_CHARS_REGEXS =
      Arrays.asList(
        Pattern.compile(".*?(\\p{Alnum}{5}).*"),
        Pattern.compile(".*?(\\p{Alnum}{4}).*"),
        Pattern.compile(".*?(\\p{Alnum}{3}).*"),
        Pattern.compile(".*?(\\p{Alnum}{2}).*"),
        Pattern.compile(".*?(\\p{Alnum}{1}).*")
      );

  protected static String get5chars(String input) {
    if (input == null) {
      return String.format("%-5s", "").replace(" ", "_");
    } else {
      String output = "";
      for (Pattern p : CONTIGUOUS_CHARS_REGEXS) {
        Matcher m = p.matcher(input);
        if (m.matches()) {
          output = m.group(1);
          break;
        }
      }
      return String.format("%-5s", output).replace(" ", "_");
    }
  }

  private String getInstanceMatchKeyValue(String name) {
    String value = null;
    if (hasMatchKeyObject(candidateInstance)) {
      value = candidateInstance.getJsonObject(MATCH_KEY).getString(name);
      value = unaccent(value);
      value = stripTrimLowercase(value);
    }
    return value != null ? value : "";
  }

  private boolean hasMatchKeyObject (JsonObject instance) {
    return (instance.containsKey(MATCH_KEY)
            && candidateInstance.getValue(MATCH_KEY) instanceof JsonObject);
  }

  private boolean hasMatchKeyAsString (JsonObject instance) {
    return (instance.containsKey(MATCH_KEY)
            && candidateInstance.getValue(MATCH_KEY) instanceof String
            && candidateInstance.getString(MATCH_KEY) != null);
  }

  /**
   * Gets first occurring date of publication
   * @return one date of publication (empty string if none found)
   */

  protected String getDateOfPublication() {
    String dateOfPublication = null;
    String dateDigits = null;
    JsonArray publication = candidateInstance.getJsonArray("publication");
    if (publication != null && !publication.getList().isEmpty() ) {
      dateOfPublication = publication.getJsonObject(0).getString("dateOfPublication");
    }
    if(dateOfPublication != null) {
      dateDigits = makeDateDigits(dateOfPublication);
    }
    if(dateDigits != null) {
      return dateDigits;
    }
    return "0000";
  }

  protected String getTypeOf() {
    return lookupTypeOf(candidateInstance.getString("instanceTypeId"));
  }

  protected String getTitlePart() {
    return makeTitlePart(getInstanceMatchKeyValue("name-of-part-section-of-work"));
  }

  protected String getTitleNumber() {
    return makeTitleNumber(getInstanceMatchKeyValue("number-of-part-section-of-work"));
  }

  protected String getGovDocNumber() {
    String number = null;
    if(candidateInstance.containsKey("classifications")) {
      JsonArray classificationList = candidateInstance.getJsonArray("classifications");
      for(Object o : classificationList) {
        JsonObject classification = (JsonObject)o;
        if(classification.containsKey("classificationTypeId") &&
            classification.getString("classificationTypeId").equals("9075b5f8-7d97-49e1-a431-73fdd468d476")) {
          number = classification.getString("classificationNumber");
          break;
        }
      }
    }
    return makeGovDocNumber(number);
  }

  protected String getAuthor() {
    String author = null;
    if(candidateInstance.containsKey("contributors")) {
      JsonArray contributorList = candidateInstance.getJsonArray("contributors");
      author = findContributorType( contributorList, PERSONAL_NAME_TYPE ) +
               findContributorType( contributorList, CORPORATE_NAME_TYPE ) +
               findContributorType( contributorList, MEETING_NAME_TYPE );
    }
    return makeAuthor(author);
  }

  protected String getTitleDates() {
    return makeTitleDates(getInstanceMatchKeyValue("inclusive-dates"));
  }

  protected String getFormatChar() {
    String medium = getInstanceMatchKeyValue("medium");
    if(medium.contains("electronic")) {
      return "e";
    }
    return "p";
  }



  protected String makeDateDigits(String date) {
    Pattern pattern = Pattern.compile("(\\d\\d\\d\\d)");
    Matcher matcher = pattern.matcher(date);
    String latestMatch = null;
    while(matcher.find()) {
      latestMatch = matcher.group(1);
    }
    if(latestMatch == null) {
      return "0000";
    }
    return latestMatch;
  }

  protected String lookupTypeOf(String instanceTypeId) {
    if(typeOfMap.containsKey(instanceTypeId)) {
      return typeOfMap.get(instanceTypeId);
    }
    return "_";
  }

  protected String makeTitlePart(String titlePart) {
    if(titlePart == null) {
      titlePart = " ";
    } else {
      titlePart = stripTrimLowercase(titlePart);
      titlePart = unaccent(titlePart);
    }
    return String.format("%-30s", titlePart).replace(" ", "_");
  }

  protected String makeGovDocNumber(String number) {
    if(number == null) {
      number = "";
    }
    //arbitrary cap at 64 chars
    if(number.length() > 64) {
      number = number.substring(0,64);
    }
    return stripTrimLowercase(number);
  }

  private String findContributorType(JsonArray list, String id) {
    for(Object o : list) {
      JsonObject contributor = (JsonObject)o;
      if(contributor.containsKey("contributorNameTypeId") &&
          contributor.getString("contributorNameTypeId").equals(id)) {
        return contributor.getString("name");
      }
    }
    return "";
  }

  protected String makeTitleNumber(String titleNumber) {
    if(titleNumber == null) {
      titleNumber = " ";
    } else {
      titleNumber = unaccent(titleNumber);
    }
    return String.format("%-10s", titleNumber).replace(" ", "_");
  }

  protected String makeAuthor(String author) {
    if(author == null) {
      author = " ";
    } else {
      author = stripTrimLowercase(author);
      unaccent(author);
    }
    return String.format("%-20s", author).replace(" ", "_");
  }

  protected String makeTitleDates(String dates) {
    if(dates == null) {
      dates = " ";
    } else {
      dates = stripTrimLowercase(dates);
    }
    return String.format("%-15s", dates).replace(" ", "_");
  }

  private static final Pattern PAGINATION_REGEX = Pattern.compile(".*?(\\d{1,4}).*");

  /**
   * Gets first occurring physical description
   * @return one physical description (empty string if none found)
   */
  public String getPhysicalDescription() {
    String physicalDescription = "";
    JsonArray physicalDescriptions = candidateInstance.getJsonArray("physicalDescriptions");
    if (physicalDescriptions != null && !physicalDescriptions.getList().isEmpty()) {
      String physicalDescriptionSource = physicalDescriptions.getList().get(0).toString();
      physicalDescriptionSource = unaccent(physicalDescriptionSource);
      Matcher m = PAGINATION_REGEX.matcher(physicalDescriptionSource);
      if (m.matches()) {
        physicalDescription = m.group(1);
      }
    }
    return String.format("%-4s", physicalDescription).replace(" ", "_");
  }


  // In order of priority,
  // pick first occuring 3, else 2, else 1 contiguous digits,
  // else first 3, else 2, else 1 contiguous characters
  private static final List<Pattern> EDITION_REGEXS =
      Arrays.asList(
        Pattern.compile(".*?(\\d{3}).*"),
        Pattern.compile(".*?(\\d{2}).*"),
        Pattern.compile(".*?(\\d{1}).*"),
        Pattern.compile(".*?(\\p{Alpha}{3}).*"),
        Pattern.compile(".*?(\\p{Alpha}{2}).*"),
        Pattern.compile(".*?(\\p{Alpha}{1}).*")
      );

  public String getEdition() {
    String edition = "";
    JsonArray editions = candidateInstance.getJsonArray("editions");
    if (editions != null && !editions.getList().isEmpty()) {
      String editionSource = editions.getList().get(0).toString();
      editionSource = unaccent(editionSource);
      Matcher m;
      for (Pattern p : EDITION_REGEXS) {
        m = p.matcher(editionSource);
        if (m.matches()) {
          edition = m.group(1);
          break;
        }
      }
    }
    return String.format("%-3s", edition).replace(" ", "_");
  }

  public String getPublisher() {
    String publisher = null;
    JsonArray publication = candidateInstance.getJsonArray("publication");
    if (publication != null && !publication.getList().isEmpty() ) {
      publisher = publication.getJsonObject(0).getString("publisher");
    }
    publisher = unaccent(publisher);
    publisher = stripTrimLowercase(publisher);
    if(publisher != null && publisher.length() > 10) {
      publisher = publisher.substring(0, 10);
    }
    if(publisher == null) {
      publisher = "";
    }
    publisher = String.format("%-10s", publisher).replace(" ", "_");
    return publisher;
  }

  public String getKey () {
    return this.matchkee;
  }

}
