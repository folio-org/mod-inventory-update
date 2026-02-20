package org.folio.inventoryupdate.importing.service.delivery.fileimport.transformation;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.utils.SecureSaxParser;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public final class InventoryXmlToInventoryJson {

  public static final Logger logger = LogManager.getLogger("InventoryXmlToInventoryJson");

  private InventoryXmlToInventoryJson() {
    throw new IllegalStateException("Utility class");
  }

  public static JsonObject convert(String xmlStr) {
    JsonObject genericJson = parseXmlToJson(xmlStr);
    JsonObject inventoryJson = makeInventoryJson(genericJson);
    inventoryJson.remove("original");
    return inventoryJson;
  }

  public static JsonObject parseXmlToJson(String xmlStr) {
    try {
      XmlToJsonHandler handler = new XmlToJsonHandler();
      SecureSaxParser.get().parse(new InputSource(new StringReader(xmlStr)), handler);
      return handler.getResultAsJson();
    } catch (ParserConfigurationException | SAXException | IOException e) {
      logger.error("Error parsing XML to JSON: {}", e.getMessage());
    }
    return new JsonObject();
  }

  private static JsonObject makeInventoryJson(JsonObject genericJson) {
    JsonObject json = new JsonObject();
    if (genericJson.containsKey("children")) {
      return makeInventoryJsonObjects(json, genericJson.getJsonArray("children").getJsonObject(0));
    }
    return json;
  }

  private static JsonObject makeInventoryJsonObjects(JsonObject toJson, JsonObject genericJson) {
    String propertyName = genericJson.getString("name");
    if (genericJson.containsKey("children")) {
      JsonArray childProperties = genericJson.getJsonArray("children");
      if (childProperties.getJsonObject(0).getString("name").equals("arr")) {
        JsonArray toArray = new JsonArray();
        toJson.put(genericJson.getString("name"), toArray);
        makeInventoryJsonArray(toArray, childProperties);
      } else {
        if (propertyName.equals("record")) {
          for (Object childProperty : childProperties) {
            makeInventoryJsonObjects(toJson, (JsonObject) childProperty);
          }
        } else {
          JsonObject toProperty = new JsonObject();
          toJson.put(propertyName, toProperty);
          for (Object childProperty : childProperties) {
            makeInventoryJsonObjects(toProperty, (JsonObject) childProperty);
          }
        }
      }
    } else if (genericJson.containsKey("text")) {
      toJson.put(propertyName, genericJson.getString("text"));
    }
    return toJson;
  }

  private static void makeInventoryJsonArray(JsonArray toJsonArray, JsonArray genericJsonArray) {
    for (Object o : genericJsonArray) {
      JsonObject element = (JsonObject) o;
      if (element.containsKey("children")) {
        for (Object child : element.getJsonArray("children")) {
          if (((JsonObject) child).containsKey("children")) {
            // array of objects
            JsonObject arrayElement = new JsonObject();
            JsonArray children = ((JsonObject) child).getJsonArray("children");
            for (Object prop : children) {
              makeInventoryJsonObjects(arrayElement, (JsonObject) prop);
            }
            toJsonArray.add(arrayElement);
          } else if (((JsonObject) child).containsKey("text")) {
            // array of strings
            toJsonArray.add(((JsonObject) child).getString("text"));
          }
        }
      }
    }
  }

  // SAX parser, XML-to-JSON.
  public static class XmlToJsonHandler extends DefaultHandler {
    private final Deque<Map<String, Object>> stack = new ArrayDeque<>();
    private Map<String, Object> currentData = new HashMap<>();

    @Override
    public void startElement(String uri, String localName, String qqName, Attributes attributes) {
      Map<String, Object> element = new HashMap<>();
      element.put("name", qqName);
      if (!stack.isEmpty()) {
        stack.peek().computeIfAbsent("children", k -> new ArrayList<Map<String, Object>>());
        ((List<Map<String, Object>>) stack.peek().get("children")).add(element);
      }
      stack.push(element);
    }

    @Override
    public void endElement(String uri, String localName, String qqName) {
      if (!stack.isEmpty()) {
        currentData = stack.pop();
      }
    }

    @Override
    public void characters(char[] ch, int start, int length) {
      String content = new String(ch, start, length).trim();
      if (!content.isEmpty()) {
        stack.peek().putIfAbsent("text", "");
        // Concatenating because text with entities (i.e. &gt;) is reported in parts..
        stack.peek().put("text", stack.peek().get("text") + content);
      }
    }

    public JsonObject getResultAsJson() {
      return new JsonObject(currentData);
    }
  }
}



