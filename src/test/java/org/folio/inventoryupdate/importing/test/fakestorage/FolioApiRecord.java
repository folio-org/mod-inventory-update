package org.folio.inventoryupdate.importing.test.fakestorage;


import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FolioApiRecord extends FakeRecord {
    public static final Logger logger = LogManager.getLogger("folioApiRecord");

    public FolioApiRecord(JsonObject folioApiRecord) {
        recordJson = folioApiRecord;
    }

    public FolioApiRecord() {
      recordJson = new JsonObject();
    }



    public boolean match(String query) {
        logger.debug("Matching {} with query {}", recordJson, query);
        Pattern orListPattern = Pattern.compile("[(]?(.*)==\\(([^)]*)\\)[)]?");
        Matcher orListMatcher = orListPattern.matcher(query);
        if (orListMatcher.find()) {
            logger.debug("OR list found");
            String key = orListMatcher.group(1);
            String[] values = orListMatcher.group(2).split(" [oO][rR] ");
            for (String value : values) {
                if (value.replace("\"","").equals(recordJson.getString(key))) {
                    return true;
                }
            }
        } else if (query.contains(" and ") || query.contains(" AND ")) {
            String trimmed = query.replace("(", "").replace(")", "");
            String[] sections = trimmed.split(" [aA][nN][dD] ");
            logger.debug(
                    "andSections: {}", ( sections.length > 1 ? sections[0] + ", " + sections[1] : sections[0] ));
            for (int i = 0; i < sections.length; i++) {
                if (sections[i].contains(" not ")) {
                    Pattern pattern = Pattern.compile(" not ([^ ]+)");
                    Matcher matcher = pattern.matcher(sections[i]);
                    if (matcher.find()) {
                        String notCriterion = matcher.group(1);
                        String[] equalityParts = notCriterion.split("==");
                        String key = equalityParts[0];
                        String value = equalityParts.length > 1 ? equalityParts[1].replace("\"", "") : "";
                        if (recordJson.getString(key) != null && recordJson.getString(key).equals(
                                value)) {
                            logger.debug("NOT query, no match for {} not equal to {} in {}", key, value, recordJson);
                            return false;
                        } else {
                            logger.debug("NOT query, have match for {} not equal to {} in {}", key, value, recordJson);
                        }
                    }
                }
                String[] queryParts = sections[i].split("==?");
                logger.debug("query: {}", query);
                logger.debug("queryParts[0]: {}", queryParts[0]);
                String key = queryParts[0];
                String value = queryParts.length > 1 ? queryParts[1].replace("\"", "") : "";
                logger.debug("key: {}", key);
                logger.debug("value: {}", value);
                logger.debug("recordJson.getValue(key): {}", recordJson.getValue(key));
                logger.debug(
                        "Query parameter [{}] matches record property [{} ({})] ?: {}",
                    value, key, recordJson.getValue(key),
                    (recordJson.getValue(key) != null && recordJson.getValue(key).equals(value) ));
                if (recordJson.getValue(key) == null || !recordJson.getValue(key).equals(value)) {
                  return false;
                }
            }
            return true;
        } else if (query.contains(" or ") || query.contains(" OR ")) {
          String trimmed = query.replace("(", "").replace(")", "");
          String[] sections = trimmed.split(" [oO][rR] ");
          logger.debug(
              "orSections: {}", ( sections.length > 1 ? sections[0] + ", " + sections[1] : sections[0] ));

          for (int i = 0; i < sections.length; i++) {
            if (sections[i].contains("@identifierTypeId")) {
              if (matchIdentifierQuery(sections[i])) {
                logger.debug("Have match for {} in {}", sections[i], recordJson);
                return true;
              } else {
                logger.debug("No match for {} in {}", sections[i], recordJson);
              }
            }

            String[] queryParts = sections[i].split("==");
            logger.debug("query: {}", query);
            logger.debug("queryParts[0]: {}", queryParts[0]);
            String key = queryParts[0];
            String value = queryParts.length > 1 ? queryParts[1].replace("\"", "") : "";
            logger.debug("key: {}", key);
            logger.debug("value: {}", value);
            logger.debug("recordJson.getString(key): {}", recordJson.getString(key));
            logger.debug(
                "Query parameter [{}] matches record property [{} ({})] ?: {}",
                value, key, recordJson.getString(key),
                (recordJson.getString(key) != null && recordJson.getString(key).equals(value) ));
            if (recordJson.getString(key) != null && recordJson.getString(key).equals(value)) {
              return true;
            }
          }
        } else {
          String trimmed = query.replace("(", "").replace(")", "");
          if (trimmed.contains("@identifierTypeId")) {
            if (matchIdentifierQuery(trimmed)) {
              logger.debug("Have match for {} in {}", trimmed, recordJson);
              return true;
            } else {
              logger.debug("No match for {} in {}", trimmed, recordJson);
            }
          }
          String[] queryParts = trimmed.split("==");
          logger.debug("query: {}", query);
          logger.debug("queryParts[0]: {}", queryParts[0]);
          String key = queryParts[0];
          String value = queryParts.length > 1 ? queryParts[1].replace("\"", "") : "";
          logger.debug("key: {}", key);
          logger.debug("value: {}", value);
          logger.debug("recordJson.getString(key): {}", recordJson.getString(key));
          logger.debug(
              "Query parameter [{}] matches record property [{} ({})] ?: {}",
              value, key, recordJson.getString(key),
              (recordJson.getString(key) != null && recordJson.getString(key).equals(value) ));
          return recordJson.getString(key) != null && recordJson.getString(key).equals(value);

        }
        return false;
    }

  public boolean matchIdentifierQuery (String identifierQuery) {
    String[] sections = identifierQuery.split(" ");
    String[] typeCriterion = sections[1].split("=");
    String identifierType = typeCriterion[2].replace("\"","");
    String value = sections[2].replace("\"","");
    logger.debug("Identifier query received, identifierType: [{}}], value: [{}}]", identifierType, value);
    if (recordJson.containsKey("identifiers")) {
      for (Object o : recordJson.getJsonArray("identifiers")) {
        JsonObject identifier = (JsonObject) o;
        if (identifier.getString("identifierTypeId").equals(identifierType)
            &&
            identifier.getString("value").equals(value)) {
          return true;
        }
      }
    }
    return false;
  }

}
